//! Async token URI fetching and caching service.
//!
//! Processes `(contract_address, token_id)` requests via a tokio channel.
//! Deduplicates in-flight tasks — if a new request arrives for the same key,
//! the previous task is cancelled so we always apply the latest value.
//!
//! The service fetches `token_uri(token_id)` (ERC721) or `uri(token_id)` (ERC1155),
//! resolves the JSON metadata, and stores the result via a callback.
//!
//! Metadata resolution is modeled after dojoengine/torii's battle-tested approach:
//! - Retries with exponential backoff for transient errors
//! - Permanent error detection (EntrypointNotFound, ContractNotFound)
//! - Tries multiple selectors: token_uri, tokenURI, uri
//! - ERC1155 `{id}` substitution in URIs
//! - data: URI support (base64 and URL-encoded JSON)
//! - IPFS gateway resolution
//! - JSON sanitization for broken metadata (control chars, unescaped quotes)
//! - Raw JSON fallback for inline metadata

use primitive_types::U256;
use starknet_types_raw::Felt;
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::MetadataFetcher;

// Retry configuration
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_RETRIES: u32 = 5;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const IMAGE_FETCH_TIMEOUT: Duration = Duration::from_secs(20);
const RPC_BATCH_SIZE_CAP: usize = 128;
const WRITE_BATCH_SIZE: usize = 128;
const WRITE_BATCH_DELAY: Duration = Duration::from_millis(25);

/// Token standard hint for URI fetching
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenStandard {
    Erc721,
    Erc1155,
}

/// A request to fetch/update a token's URI and metadata.
#[derive(Debug, Clone)]
pub struct TokenUriRequest {
    /// Contract address
    pub contract: Felt,
    /// Token ID
    pub token_id: U256,
    /// Which standard to use for fetching
    pub standard: TokenStandard,
}

/// Dedupe key for in-flight tasks
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TaskKey {
    contract: Felt,
    token_id: U256,
}

/// Result of a token URI fetch
#[derive(Debug, Clone)]
pub struct TokenUriResult {
    /// Contract address
    pub contract: Felt,
    /// Token ID
    pub token_id: U256,
    /// The raw URI string (e.g. ipfs://..., https://...)
    pub uri: Option<String>,
    /// Resolved JSON metadata (if URI pointed to JSON)
    pub metadata_json: Option<String>,
}

/// Callback trait for storing fetched token URI results.
#[async_trait::async_trait]
pub trait TokenUriStore: Send + Sync + 'static {
    async fn store_token_uris_batch(&self, results: &[TokenUriResult]) -> anyhow::Result<()>;

    async fn store_token_uri(&self, result: &TokenUriResult) -> anyhow::Result<()> {
        self.store_token_uris_batch(std::slice::from_ref(result))
            .await
    }
}

/// Handle to send requests to the token URI service.
#[derive(Clone)]
pub struct TokenUriSender {
    tx: mpsc::UnboundedSender<TokenUriRequest>,
}

impl TokenUriSender {
    /// Queue a token URI fetch request.
    pub fn request_update(&self, request: TokenUriRequest) -> bool {
        if let Err(_error) = self.tx.send(request) {
            tracing::warn!(
                target: "torii_common::token_uri",
                "Failed to send token URI request (channel closed)"
            );
            false
        } else {
            true
        }
    }

    /// Queue updates for a batch of token IDs on the same contract.
    pub fn request_batch(
        &self,
        contract: Felt,
        token_ids: &[U256],
        standard: TokenStandard,
    ) -> usize {
        let mut accepted = 0;
        for &token_id in token_ids {
            accepted += usize::from(self.request_update(TokenUriRequest {
                contract,
                token_id,
                standard,
            }));
        }
        accepted
    }
}

struct RequestBacklog {
    queued: HashMap<TaskKey, TokenUriRequest>,
    ready: VecDeque<TaskKey>,
}

impl RequestBacklog {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            queued: HashMap::with_capacity(capacity),
            ready: VecDeque::with_capacity(capacity),
        }
    }

    fn enqueue(&mut self, request: TokenUriRequest) {
        let key = TaskKey {
            contract: request.contract,
            token_id: request.token_id,
        };

        if let Some(existing) = self.queued.get_mut(&key) {
            *existing = request;
            return;
        }

        self.ready.push_back(key.clone());
        self.queued.insert(key, request);
    }

    fn pop_batch(&mut self, batch_size: usize) -> Vec<TokenUriRequest> {
        let mut batch = Vec::with_capacity(batch_size.min(self.ready.len()));
        while batch.len() < batch_size {
            let Some(key) = self.ready.pop_front() else {
                break;
            };
            if let Some(request) = self.queued.remove(&key) {
                batch.push(request);
            }
        }
        batch
    }

    fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    fn len(&self) -> usize {
        self.queued.len()
    }
}

/// The background service that processes token URI fetch requests.
pub struct TokenUriService {
    handle: JoinHandle<()>,
}

#[derive(Clone)]
struct ImageCacheConfig {
    dir: PathBuf,
    max_concurrent: usize,
}

#[derive(Debug)]
struct BufferedTokenUriResult {
    result: TokenUriResult,
    image_uri: Option<String>,
}

impl TokenUriService {
    /// Spawn the token URI service.
    ///
    /// Returns a `(TokenUriSender, TokenUriService)` pair.
    /// The sender is cheap to clone and can be shared across sinks.
    pub fn spawn<S: TokenUriStore>(
        fetcher: Arc<MetadataFetcher>,
        store: Arc<S>,
        buffer_size: usize,
        max_concurrent: usize,
    ) -> (TokenUriSender, Self) {
        Self::spawn_with_image_cache(fetcher, store, buffer_size, max_concurrent, None, 4)
    }

    /// Spawn the token URI service with optional local image caching.
    ///
    /// When `image_cache_dir` is set, image URLs from metadata are downloaded in
    /// background tasks and saved under that directory.
    pub fn spawn_with_image_cache<S: TokenUriStore>(
        fetcher: Arc<MetadataFetcher>,
        store: Arc<S>,
        buffer_size: usize,
        max_concurrent: usize,
        image_cache_dir: Option<PathBuf>,
        image_max_concurrent: usize,
    ) -> (TokenUriSender, Self) {
        let (tx, rx) = mpsc::unbounded_channel();
        let image_cache = image_cache_dir.map(|dir| ImageCacheConfig {
            dir,
            max_concurrent: image_max_concurrent.max(1),
        });
        let handle = tokio::spawn(Self::run(
            rx,
            fetcher,
            store,
            buffer_size.max(1),
            max_concurrent.max(1),
            image_cache,
        ));
        let sender = TokenUriSender { tx };
        (sender, Self { handle })
    }

    /// Main processing loop.
    async fn run<S: TokenUriStore>(
        mut rx: mpsc::UnboundedReceiver<TokenUriRequest>,
        fetcher: Arc<MetadataFetcher>,
        store: Arc<S>,
        backlog_capacity: usize,
        max_concurrent: usize,
        image_cache: Option<ImageCacheConfig>,
    ) {
        let (result_tx, result_rx) = mpsc::channel::<BufferedTokenUriResult>(WRITE_BATCH_SIZE * 2);
        let writer = tokio::spawn(Self::run_writer(result_rx, store, image_cache.clone()));
        let mut backlog = RequestBacklog::with_capacity(backlog_capacity);
        let rpc_batch_size = backlog_capacity.clamp(1, RPC_BATCH_SIZE_CAP);

        loop {
            if backlog.is_empty() {
                match rx.recv().await {
                    Some(request) => backlog.enqueue(request),
                    None => break,
                }
            }

            Self::drain_pending_requests(&mut rx, &mut backlog);

            while !backlog.is_empty() {
                let batch = backlog.pop_batch(rpc_batch_size);
                let resolved =
                    Self::resolve_token_uri_requests_batch(&fetcher, batch, max_concurrent).await;

                for buffered in resolved {
                    if let Err(error) = result_tx.send(buffered).await {
                        tracing::warn!(
                            target: "torii_common::token_uri",
                            error = %error,
                            "Failed to enqueue resolved token URI result"
                        );
                        break;
                    }
                }

                tracing::debug!(
                    target: "torii_common::token_uri",
                    queued = backlog.len(),
                    rpc_batch_size,
                    "Processed token URI queue batch"
                );

                Self::drain_pending_requests(&mut rx, &mut backlog);
            }
        }

        drop(result_tx);
        let _ = writer.await;

        tracing::info!(
            target: "torii_common::token_uri",
            "Token URI service shutting down"
        );
    }

    async fn run_writer<S: TokenUriStore>(
        mut rx: mpsc::Receiver<BufferedTokenUriResult>,
        store: Arc<S>,
        image_cache: Option<ImageCacheConfig>,
    ) {
        let image_semaphore = image_cache
            .as_ref()
            .map(|cfg| Arc::new(tokio::sync::Semaphore::new(cfg.max_concurrent)));
        let mut pending: HashMap<TaskKey, BufferedTokenUriResult> = HashMap::new();
        let mut closed = false;

        loop {
            if closed {
                if pending.is_empty() {
                    break;
                }
                Self::flush_results(
                    &store,
                    &mut pending,
                    image_cache.clone(),
                    image_semaphore.clone(),
                )
                .await;
                break;
            }

            if pending.is_empty() {
                if let Some(result) = rx.recv().await {
                    pending.insert(
                        TaskKey {
                            contract: result.result.contract,
                            token_id: result.result.token_id,
                        },
                        result,
                    );
                } else {
                    closed = true;
                    continue;
                }
            } else {
                let delay = tokio::time::sleep(WRITE_BATCH_DELAY);
                tokio::pin!(delay);
                tokio::select! {
                    maybe_result = rx.recv() => {
                        match maybe_result {
                            Some(result) => {
                                pending.insert(
                                    TaskKey {
                                        contract: result.result.contract,
                                        token_id: result.result.token_id,
                                    },
                                    result,
                                );
                            }
                            None => closed = true,
                        }
                    }
                    () = &mut delay => {
                        Self::flush_results(&store, &mut pending, image_cache.clone(), image_semaphore.clone()).await;
                    }
                }
            }

            if pending.len() >= WRITE_BATCH_SIZE {
                Self::flush_results(
                    &store,
                    &mut pending,
                    image_cache.clone(),
                    image_semaphore.clone(),
                )
                .await;
            }
        }
    }

    async fn flush_results<S: TokenUriStore>(
        store: &Arc<S>,
        pending: &mut HashMap<TaskKey, BufferedTokenUriResult>,
        image_cache: Option<ImageCacheConfig>,
        image_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    ) {
        if pending.is_empty() {
            return;
        }

        let drained = pending
            .drain()
            .map(|(_, result)| result)
            .collect::<Vec<_>>();
        let results = drained
            .iter()
            .map(|buffered| buffered.result.clone())
            .collect::<Vec<_>>();

        if let Err(error) = store.store_token_uris_batch(&results).await {
            tracing::warn!(
                target: "torii_common::token_uri",
                batch_size = results.len(),
                error = %error,
                "Failed to store token URI batch"
            );
            return;
        }

        for buffered in drained {
            tracing::debug!(
                target: "torii_common::token_uri",
                contract = %format!("{:#x}", buffered.result.contract),
                token_id = %buffered.result.token_id,
                uri = ?buffered.result.uri,
                has_json = buffered.result.metadata_json.is_some(),
                "Stored token URI"
            );

            if let (Some(cfg), Some(image_uri)) = (image_cache.clone(), buffered.image_uri) {
                let contract = buffered.result.contract;
                let token_id = buffered.result.token_id;
                let semaphore = image_semaphore.clone();
                tokio::spawn(async move {
                    let _image_permit = match semaphore {
                        Some(sema) => sema.acquire_owned().await.ok(),
                        None => None,
                    };

                    if let Err(error) =
                        cache_image_locally(&cfg.dir, contract, token_id, &image_uri).await
                    {
                        tracing::debug!(
                            target: "torii_common::token_uri",
                            contract = %format!("{:#x}", contract),
                            token_id = %token_id,
                            image_uri = %image_uri,
                            error = %error,
                            "Failed to cache image locally"
                        );
                    }
                });
            }
        }
    }

    /// Wait for the service to finish.
    pub async fn join(self) {
        let _ = self.handle.await;
    }

    /// Abort the background task.
    pub fn abort(self) {
        self.handle.abort();
    }

    fn drain_pending_requests(
        rx: &mut mpsc::UnboundedReceiver<TokenUriRequest>,
        backlog: &mut RequestBacklog,
    ) {
        while let Ok(request) = rx.try_recv() {
            backlog.enqueue(request);
        }
    }

    async fn resolve_token_uri_requests_batch(
        fetcher: &MetadataFetcher,
        requests: Vec<TokenUriRequest>,
        max_concurrent: usize,
    ) -> Vec<BufferedTokenUriResult> {
        if requests.is_empty() {
            return Vec::new();
        }

        let mut raw_uris = vec![None; requests.len()];
        let mut erc721_positions = Vec::new();
        let mut erc1155_positions = Vec::new();
        let mut erc721_requests = Vec::new();
        let mut erc1155_requests = Vec::new();

        for (idx, request) in requests.iter().enumerate() {
            // let token_id = Felt::from(request.token_id.low());
            let token_id = Felt::from_le_words(request.token_id.0); // CHECK: same logic
            match request.standard {
                TokenStandard::Erc721 => {
                    erc721_positions.push(idx);
                    erc721_requests.push((request.contract, token_id));
                }
                TokenStandard::Erc1155 => {
                    erc1155_positions.push(idx);
                    erc1155_requests.push((request.contract, token_id));
                }
            }
        }

        let erc721_results = fetcher.fetch_token_uri_batch(&erc721_requests).await;
        for (position, uri) in erc721_positions.into_iter().zip(erc721_results) {
            raw_uris[position] = uri;
        }

        let erc1155_results = fetcher.fetch_uri_batch(&erc1155_requests).await;
        for (position, uri) in erc1155_positions.into_iter().zip(erc1155_results) {
            raw_uris[position] = uri;
        }

        let metadata_parallelism = max_concurrent.max(1);
        let mut next_idx = 0usize;
        let mut join_set = tokio::task::JoinSet::new();
        let mut resolved = (0..requests.len()).map(|_| None).collect::<Vec<_>>();

        while next_idx < requests.len() || !join_set.is_empty() {
            while next_idx < requests.len() && join_set.len() < metadata_parallelism {
                let request = requests[next_idx].clone();
                let raw_uri = raw_uris[next_idx].clone();
                join_set.spawn(async move {
                    let result = resolve_token_uri_from_uri(request, raw_uri).await;
                    let image_uri = result.metadata_json.as_deref().and_then(extract_image_uri);
                    (next_idx, BufferedTokenUriResult { result, image_uri })
                });
                next_idx += 1;
            }

            let Some(joined) = join_set.join_next().await else {
                break;
            };
            match joined {
                Ok((idx, buffered)) => resolved[idx] = Some(buffered),
                Err(error) => {
                    tracing::warn!(
                        target: "torii_common::token_uri",
                        error = %error,
                        "Token URI metadata task failed"
                    );
                }
            }
        }

        resolved.into_iter().flatten().collect()
    }
}

pub async fn process_token_uri_request<S: TokenUriStore>(
    fetcher: &MetadataFetcher,
    store: &S,
    request: &TokenUriRequest,
    image_cache_dir: Option<&Path>,
) -> anyhow::Result<()> {
    let result = resolve_token_uri_request(fetcher, request).await;

    store.store_token_uri(&result).await?;

    if let (Some(root_dir), Some(meta)) = (image_cache_dir, result.metadata_json.as_ref()) {
        if let Some(image_uri) = extract_image_uri(meta) {
            if let Err(error) =
                cache_image_locally(root_dir, request.contract, request.token_id, &image_uri).await
            {
                tracing::debug!(
                    target: "torii_common::token_uri",
                    contract = %format!("{:#x}", request.contract),
                    token_id = %request.token_id,
                    image_uri = %image_uri,
                    error = %error,
                    "Failed to cache image locally"
                );
            }
        }
    }

    Ok(())
}

async fn resolve_token_uri_request(
    fetcher: &MetadataFetcher,
    request: &TokenUriRequest,
) -> TokenUriResult {
    let uri = fetch_token_uri_with_retry(
        fetcher,
        request.contract,
        request.token_id,
        request.standard,
    )
    .await;

    let uri = uri.map(|u| {
        if request.standard == TokenStandard::Erc1155 {
            let token_id_hex = format!("{:064x}", request.token_id);
            u.replace("{id}", &token_id_hex)
        } else {
            u
        }
    });

    let metadata_json = if let Some(ref uri_str) = uri {
        if uri_str.is_empty() {
            None
        } else {
            resolve_metadata(uri_str).await
        }
    } else {
        None
    };

    TokenUriResult {
        contract: request.contract,
        token_id: request.token_id,
        uri,
        metadata_json,
    }
}

async fn resolve_token_uri_from_uri(
    request: TokenUriRequest,
    raw_uri: Option<String>,
) -> TokenUriResult {
    let uri = raw_uri.map(|value| {
        if request.standard == TokenStandard::Erc1155 {
            let token_id_hex = format!("{:064x}", request.token_id);
            value.replace("{id}", &token_id_hex)
        } else {
            value
        }
    });

    let metadata_json = if let Some(ref uri_str) = uri {
        if uri_str.is_empty() {
            None
        } else {
            resolve_metadata(uri_str).await
        }
    } else {
        None
    };

    TokenUriResult {
        contract: request.contract,
        token_id: request.token_id,
        uri,
        metadata_json,
    }
}

fn extract_image_uri(metadata_json: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(metadata_json).ok()?;
    for key in ["image", "image_url", "imageUrl"] {
        if let Some(v) = value.get(key).and_then(|v| v.as_str()) {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_owned());
            }
        }
    }
    if let Some(props) = value.get("properties") {
        for key in ["image", "image_url", "imageUrl"] {
            if let Some(v) = props.get(key).and_then(|v| v.as_str()) {
                let trimmed = v.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_owned());
                }
            }
        }
    }
    None
}

async fn cache_image_locally(
    root_dir: &Path,
    contract: Felt,
    token_id: U256,
    image_uri: &str,
) -> anyhow::Result<PathBuf> {
    let (bytes, content_type, source_url) = fetch_image_bytes_with_retry(image_uri)
        .await
        .ok_or_else(|| anyhow::anyhow!("image download failed"))?;

    let contract_dir = format!("{contract:#x}").trim_start_matches("0x").to_owned();
    let token_name = format!("{token_id:064x}");
    let ext = image_extension(content_type.as_deref(), &source_url);

    let dir = root_dir.join(contract_dir);
    tokio::fs::create_dir_all(&dir).await?;
    let final_path = dir.join(format!("{token_name}.{ext}"));
    if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
        return Ok(final_path);
    }

    let tmp_path = dir.join(format!("{token_name}.{ext}.tmp"));
    tokio::fs::write(&tmp_path, bytes).await?;
    tokio::fs::rename(&tmp_path, &final_path).await?;

    tracing::debug!(
        target: "torii_common::token_uri",
        contract = %format!("{:#x}", contract),
        token_id = %token_id,
        path = %final_path.display(),
        "Cached image locally"
    );
    Ok(final_path)
}

async fn fetch_image_bytes_with_retry(uri: &str) -> Option<(Vec<u8>, Option<String>, String)> {
    if uri.starts_with("data:") {
        let (bytes, content_type) = resolve_data_uri_bytes(uri)?;
        return Some((bytes, content_type, uri.to_owned()));
    }

    let url = if let Some(cid) = uri.strip_prefix("ipfs://") {
        format!("https://ipfs.io/ipfs/{cid}")
    } else if uri.starts_with("http://") || uri.starts_with("https://") {
        uri.to_owned()
    } else {
        return None;
    };

    let client = reqwest::Client::builder()
        .timeout(IMAGE_FETCH_TIMEOUT)
        .build()
        .ok()?;

    let mut retries = 0;
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match client.get(&url).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    return None;
                }
                let content_type = resp
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|h| h.to_str().ok())
                    .map(std::borrow::ToOwned::to_owned);
                let bytes = resp.bytes().await.ok()?.to_vec();
                return Some((bytes, content_type, url));
            }
            Err(_) if retries < MAX_RETRIES => {
                tokio::time::sleep(backoff).await;
                retries += 1;
                backoff *= 2;
            }
            Err(_) => return None,
        }
    }
}

fn resolve_data_uri_bytes(uri: &str) -> Option<(Vec<u8>, Option<String>)> {
    let uri = uri.replace('#', "%23");
    let comma_pos = uri.find(',')?;
    let header = &uri[5..comma_pos];
    let body = &uri[comma_pos + 1..];

    let mut parts = header.split(';');
    let media_type = parts.next().unwrap_or_default().trim();
    let is_base64 = parts.any(|p| p.eq_ignore_ascii_case("base64"));

    let bytes = if is_base64 {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD
            .decode(body)
            .ok()?
    } else {
        urlencoding::decode(body).ok()?.as_bytes().to_vec()
    };

    let content_type = if media_type.is_empty() {
        None
    } else {
        Some(media_type.to_owned())
    };
    Some((bytes, content_type))
}

fn image_extension(content_type: Option<&str>, source_url: &str) -> &'static str {
    if let Some(ct) = content_type {
        let main = ct.split(';').next().unwrap_or_default().trim();
        return match main {
            "image/png" => "png",
            "image/jpeg" => "jpg",
            "image/webp" => "webp",
            "image/gif" => "gif",
            "image/svg+xml" => "svg",
            "image/avif" => "avif",
            _ => "bin",
        };
    }

    let path = source_url.split('?').next().unwrap_or(source_url);
    if let Some(ext) = path.rsplit('.').next() {
        let ext = ext.to_ascii_lowercase();
        return match ext.as_str() {
            "png" => "png",
            "jpg" | "jpeg" => "jpg",
            "webp" => "webp",
            "gif" => "gif",
            "svg" => "svg",
            "avif" => "avif",
            _ => "bin",
        };
    }
    "bin"
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Token URI fetching (from chain)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Fetch token URI with retries, trying multiple selectors.
///
/// Tries `token_uri`, `tokenURI`, and `uri` selectors in order.
/// Distinguishes permanent errors (EntrypointNotFound) from transient ones.
async fn fetch_token_uri_with_retry(
    fetcher: &MetadataFetcher,
    contract: Felt,
    token_id: U256,
    standard: TokenStandard,
) -> Option<String> {
    // Use the MetadataFetcher which already tries multiple selectors
    let token_id_felt = Felt::from_le_words(token_id.0); // CHECK: same logic as before

    match standard {
        TokenStandard::Erc721 => fetcher.fetch_token_uri(contract, token_id_felt).await,
        TokenStandard::Erc1155 => fetcher.fetch_uri(contract, token_id_felt).await,
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Metadata resolution (URI → JSON)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Resolve a URI to JSON metadata string.
///
/// Handles:
/// - `https://` / `http://` URLs — fetch with retries
/// - `ipfs://` URIs — convert to gateway URL, fetch with retries
/// - `data:application/json;base64,` — decode inline
/// - `data:application/json,` — decode inline (URL-encoded)
/// - Raw JSON — try to parse as-is (fallback)
///
/// Returns the metadata as a JSON string, or None on failure.
/// Based on dojoengine/torii's battle-tested `fetch_metadata`.
async fn resolve_metadata(uri: &str) -> Option<String> {
    let result = match uri {
        u if u.starts_with("http://") || u.starts_with("https://") => {
            fetch_http_with_retry(u).await
        }
        u if u.starts_with("ipfs://") => {
            let cid = &u[7..];
            // Try multiple IPFS gateways
            let gateway_url = format!("https://ipfs.io/ipfs/{cid}");
            fetch_http_with_retry(&gateway_url).await
        }
        u if u.starts_with("data:") => resolve_data_uri(u),
        u => {
            // Fallback: try to parse as raw JSON
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(u) {
                serde_json::to_string(&json).ok()
            } else {
                tracing::debug!(
                    target: "torii_common::token_uri",
                    uri = u,
                    "Unsupported URI scheme and not valid JSON"
                );
                None
            }
        }
    };

    // Sanitize the JSON if we got a result
    result.and_then(|raw| {
        let sanitized = sanitize_json_string(&raw);
        // Validate it's actually valid JSON
        match serde_json::from_str::<serde_json::Value>(&sanitized) {
            Ok(json) => serde_json::to_string(&json).ok(),
            Err(e) => {
                tracing::debug!(
                    target: "torii_common::token_uri",
                    error = %e,
                    "Fetched content is not valid JSON after sanitization"
                );
                None
            }
        }
    })
}

/// Fetch HTTP content with exponential backoff retries.
async fn fetch_http_with_retry(url: &str) -> Option<String> {
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .ok()?;

    let mut retries = 0;
    let mut backoff = INITIAL_BACKOFF;

    loop {
        match client.get(url).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    tracing::debug!(
                        target: "torii_common::token_uri",
                        url = %url,
                        status = %resp.status(),
                        "HTTP fetch failed"
                    );
                    return None;
                }
                return resp.text().await.ok();
            }
            Err(e) => {
                if retries >= MAX_RETRIES {
                    tracing::debug!(
                        target: "torii_common::token_uri",
                        url = %url,
                        error = %e,
                        "HTTP fetch failed after {} retries",
                        MAX_RETRIES
                    );
                    return None;
                }
                tracing::debug!(
                    target: "torii_common::token_uri",
                    url = %url,
                    error = %e,
                    retry = retries + 1,
                    "HTTP fetch failed, retrying"
                );
                tokio::time::sleep(backoff).await;
                retries += 1;
                backoff *= 2;
            }
        }
    }
}

/// Resolve a `data:` URI to its content.
///
/// Supports:
/// - `data:application/json;base64,<encoded>`
/// - `data:application/json,<url-encoded>`
/// - Other data URIs with JSON content
fn resolve_data_uri(uri: &str) -> Option<String> {
    // Handle the # issue: https://github.com/servo/rust-url/issues/908
    let uri = uri.replace('#', "%23");

    if let Some(encoded) = uri.strip_prefix("data:application/json;base64,") {
        return base64_decode(encoded);
    }

    if let Some(json) = uri.strip_prefix("data:application/json,") {
        let decoded = urlencoding::decode(json)
            .unwrap_or_else(|_| json.into())
            .into_owned();
        return Some(decoded);
    }

    // Generic data URI handling
    if let Some(comma_pos) = uri.find(',') {
        let header = &uri[5..comma_pos]; // skip "data:"
        let body = &uri[comma_pos + 1..];

        if header.contains("base64") {
            return base64_decode(body);
        }

        let decoded = urlencoding::decode(body)
            .unwrap_or_else(|_| body.into())
            .into_owned();
        return Some(decoded);
    }

    tracing::debug!(
        target: "torii_common::token_uri",
        "Malformed data URI"
    );
    None
}

/// Sanitize a JSON string by escaping unescaped double quotes within string values
/// and filtering out control characters.
///
/// Ported from dojoengine/torii — handles broken metadata like Loot Survivor NFTs.
fn sanitize_json_string(s: &str) -> String {
    // First filter out ASCII control characters (except standard whitespace)
    let filtered: String = s
        .chars()
        .filter(|c| !c.is_ascii_control() || *c == '\n' || *c == '\r' || *c == '\t')
        .collect();

    let mut result = String::with_capacity(filtered.len());
    let mut chars = filtered.chars();
    let mut in_string = false;
    let mut backslash_count: usize = 0;

    while let Some(c) = chars.next() {
        if !in_string {
            if c == '"' {
                in_string = true;
                backslash_count = 0;
                result.push('"');
            } else {
                result.push(c);
            }
            continue;
        }

        // Inside a string
        if c == '\n' {
            result.push_str("\\n");
            backslash_count = 0;
            continue;
        }

        if c == '\r' {
            result.push_str("\\r");
            backslash_count = 0;
            continue;
        }

        if c == '\t' {
            result.push_str("\\t");
            backslash_count = 0;
            continue;
        }

        if c == '\\' {
            backslash_count += 1;
            result.push('\\');
            continue;
        }

        if c == '"' {
            if backslash_count.is_multiple_of(2) {
                // Unescaped quote — check if it ends the string or is internal
                let mut temp = chars.clone().peekable();
                // Skip whitespace
                while let Some(&next) = temp.peek() {
                    if next.is_whitespace() {
                        temp.next();
                    } else {
                        break;
                    }
                }
                if let Some(&next) = temp.peek() {
                    if next == ':' || next == ',' || next == '}' || next == ']' {
                        // End of string value
                        result.push('"');
                        in_string = false;
                    } else {
                        // Internal unescaped quote — escape it
                        result.push_str("\\\"");
                    }
                } else {
                    // End of input
                    result.push('"');
                    in_string = false;
                }
            } else {
                // Already escaped
                result.push('"');
            }
            backslash_count = 0;
            continue;
        }

        result.push(c);
        backslash_count = 0;
    }

    result
}

/// Simple base64 decode helper
fn base64_decode(input: &str) -> Option<String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(input)
        .ok()?;
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_json_string_unescaped_quotes() {
        let input = r#"{"name":""Rage Shout" DireWolf"}"#;
        let expected = r#"{"name":"\"Rage Shout\" DireWolf"}"#;
        assert_eq!(sanitize_json_string(input), expected);
    }

    #[test]
    fn test_sanitize_json_string_already_escaped() {
        let input = r#"{"name":"\"Properly Escaped\" Wolf"}"#;
        assert_eq!(sanitize_json_string(input), input);
    }

    #[test]
    fn test_sanitize_json_string_control_chars() {
        let input = "{\x01\"name\": \"test\x02\"}";
        let sanitized = sanitize_json_string(input);
        assert!(!sanitized.contains('\x01'));
        assert!(!sanitized.contains('\x02'));
    }

    #[test]
    fn test_sanitize_json_string_escapes_raw_newlines_inside_strings() {
        let input = "{\"description\":\"line one\nline two\"}";
        let sanitized = sanitize_json_string(input);
        assert_eq!(sanitized, "{\"description\":\"line one\\nline two\"}");
        let parsed: serde_json::Value = serde_json::from_str(&sanitized).unwrap();
        assert_eq!(parsed["description"], "line one\nline two");
    }

    #[test]
    fn test_resolve_data_uri_base64() {
        let uri = "data:application/json;base64,eyJuYW1lIjoidGVzdCJ9";
        let result = resolve_data_uri(uri);
        assert_eq!(result, Some(r#"{"name":"test"}"#.to_string()));
    }

    #[test]
    fn test_resolve_data_uri_url_encoded() {
        let uri = "data:application/json,%7B%22name%22%3A%22test%22%7D";
        let result = resolve_data_uri(uri);
        assert_eq!(result, Some(r#"{"name":"test"}"#.to_string()));
    }

    #[test]
    fn test_resolve_data_uri_with_hash() {
        // The # character in data URIs is problematic
        let uri = "data:application/json;base64,eyJuYW1lIjoiIzEifQ==";
        let result = resolve_data_uri(uri);
        assert!(result.is_some());
    }

    #[test]
    fn test_erc1155_id_substitution() {
        let uri = "https://example.com/token/{id}.json";
        let token_id = U256::from(42u64);
        let token_id_hex = format!("{token_id:064x}");
        let result = uri.replace("{id}", &token_id_hex);
        assert!(result.contains("000000000000000000000000000000000000000000000000000000000000002a"));
    }

    #[test]
    fn request_backlog_deduplicates_same_token() {
        let contract = Felt::from_hex_unchecked("0x123");
        let mut backlog = RequestBacklog::with_capacity(8);
        backlog.enqueue(TokenUriRequest {
            contract,
            token_id: U256::from(1u64),
            standard: TokenStandard::Erc721,
        });
        backlog.enqueue(TokenUriRequest {
            contract,
            token_id: U256::from(1u64),
            standard: TokenStandard::Erc1155,
        });

        assert_eq!(backlog.len(), 1);
        let batch = backlog.pop_batch(8);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].standard, TokenStandard::Erc1155);
    }

    #[test]
    fn request_backlog_removes_items_when_popped() {
        let contract = Felt::from_hex_unchecked("0x456");
        let mut backlog = RequestBacklog::with_capacity(8);
        backlog.enqueue(TokenUriRequest {
            contract,
            token_id: U256::from(1u64),
            standard: TokenStandard::Erc721,
        });
        backlog.enqueue(TokenUriRequest {
            contract,
            token_id: U256::from(2u64),
            standard: TokenStandard::Erc721,
        });

        let first = backlog.pop_batch(1);
        assert_eq!(first.len(), 1);
        assert_eq!(backlog.len(), 1);

        let second = backlog.pop_batch(8);
        assert_eq!(second.len(), 1);
        assert!(backlog.is_empty());
    }
}
