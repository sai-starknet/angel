// Example: HTTP-Only Sink
//
// This demonstrates a sink with HTTP REST endpoints but no gRPC service:
// - In-memory storage
// - Custom HTTP routes
// - No EventBus publishing
// - No gRPC service
//
// Use case: REST API for queries without real-time streaming
//
// Run: cargo run --example http_only_sink

use anyhow::Result;
use serde::{Deserialize, Serialize};
use starknet_types_raw::Felt;
use std::sync::{Arc, RwLock};
use torii::axum::extract::State;
use torii::axum::http::StatusCode;
use torii::axum::routing::{get, post};
use torii::axum::{Json, Router};
use torii::etl::envelope::{Envelope, TypeId, TypedBody};
use torii::etl::extractor::ExtractionBatch;
use torii::etl::sink::{EventBus, Sink, SinkContext, TopicInfo};
use torii::etl::{Decoder, EventContext};
use torii::{async_trait, run, ToriiConfig};
use torii_types::event::StarknetEvent;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 1. DEFINE EVENT TYPE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Clone, Serialize, Deserialize)]
pub struct StoredEvent {
    pub id: u64,
    pub from_address: String,
    pub block_number: u64,
    pub timestamp: i64,
}

// TypedBody implementation
impl TypedBody for StoredEvent {
    fn envelope_type_id(&self) -> TypeId {
        TypeId::new("stored.event")
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 2. IN-MEMORY STORE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Clone)]
pub struct EventStore {
    events: Arc<RwLock<Vec<StoredEvent>>>,
}

impl Default for EventStore {
    fn default() -> Self {
        Self::new()
    }
}

impl EventStore {
    pub fn new() -> Self {
        Self {
            events: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn add(&self, event: StoredEvent) {
        self.events.write().unwrap().push(event);
    }

    pub fn get_all(&self) -> Vec<StoredEvent> {
        self.events.read().unwrap().clone()
    }

    pub fn count(&self) -> usize {
        self.events.read().unwrap().len()
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 3. HTTP-ONLY SINK
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

pub struct HttpSink {
    store: EventStore,
}

impl Default for HttpSink {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpSink {
    pub fn new() -> Self {
        Self {
            store: EventStore::new(),
        }
    }

    pub fn generate_sample_events() -> Vec<StarknetEvent> {
        vec![
            StarknetEvent {
                from_address: Felt::from_hex("0xaabbccdd").unwrap(),
                keys: vec![Felt::from_hex("0x1").unwrap()],
                data: vec![Felt::from_hex("0x100").unwrap()],
                block_number: 0,
                transaction_hash: Felt::ZERO,
            },
            StarknetEvent {
                from_address: Felt::from_hex("0x11223344").unwrap(),
                keys: vec![Felt::from_hex("0x2").unwrap()],
                data: vec![Felt::from_hex("0x200").unwrap()],
                block_number: 0,
                transaction_hash: Felt::ZERO,
            },
            StarknetEvent {
                from_address: Felt::from_hex("0x55667788").unwrap(),
                keys: vec![Felt::from_hex("0x3").unwrap()],
                data: vec![Felt::from_hex("0x300").unwrap()],
                block_number: 0,
                transaction_hash: Felt::ZERO,
            },
        ]
    }
}

#[async_trait]
impl Sink for HttpSink {
    fn name(&self) -> &'static str {
        "http"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("stored.event")]
    }

    fn topics(&self) -> Vec<TopicInfo> {
        // No topics - HTTP only!
        vec![]
    }

    async fn initialize(
        &mut self,
        _event_bus: Arc<EventBus>,
        _context: &SinkContext,
    ) -> Result<()> {
        tracing::info!("HttpSink initialized (HTTP-only, no EventBus publishing)");
        Ok(())
    }

    async fn process(&self, envelopes: &[Envelope], _batch: &ExtractionBatch) -> Result<()> {
        for envelope in envelopes {
            if envelope.type_id != TypeId::new("stored.event") {
                continue;
            }

            if let Some(stored_event) = envelope.downcast_ref::<StoredEvent>() {
                // Just store it - no EventBus publishing
                self.store.add(stored_event.clone());

                tracing::info!(
                    "📦 Stored event #{} from {} at block {}",
                    stored_event.id,
                    stored_event.from_address,
                    stored_event.block_number
                );
            }
        }

        Ok(())
    }

    fn build_routes(&self) -> Router {
        // HTTP ROUTES - REST API for queries
        Router::new()
            .route("/http/events", get(get_events))
            .route("/http/count", get(get_count))
            .route("/http/clear", post(clear_events))
            .with_state(self.store.clone())
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 4. HTTP HANDLERS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Serialize)]
struct EventsResponse {
    events: Vec<StoredEvent>,
    total: usize,
}

async fn get_events(State(store): State<EventStore>) -> Json<EventsResponse> {
    let events = store.get_all();
    let total = events.len();
    Json(EventsResponse { events, total })
}

#[derive(Serialize)]
struct CountResponse {
    count: usize,
}

async fn get_count(State(store): State<EventStore>) -> Json<CountResponse> {
    Json(CountResponse {
        count: store.count(),
    })
}

#[derive(Serialize)]
struct ClearResponse {
    message: String,
    cleared: usize,
}

async fn clear_events(State(store): State<EventStore>) -> (StatusCode, Json<ClearResponse>) {
    let count = store.count();
    store.events.write().unwrap().clear();
    (
        StatusCode::OK,
        Json(ClearResponse {
            message: "Events cleared".to_string(),
            cleared: count,
        }),
    )
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 5. DECODER
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

pub struct HttpDecoder {
    counter: Arc<std::sync::atomic::AtomicU64>,
}

impl Default for HttpDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpDecoder {
    pub fn new() -> Self {
        Self {
            counter: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }
}

#[async_trait]
impl Decoder for HttpDecoder {
    fn decoder_name(&self) -> &'static str {
        "http"
    }

    async fn decode(
        &self,
        _keys: &[Felt],
        _data: &[Felt],
        context: EventContext,
    ) -> Result<Vec<Envelope>> {
        let id = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let stored_event = StoredEvent {
            id,
            from_address: format!("{:#x}", context.from_address),
            block_number: context.block_number,
            timestamp: chrono::Utc::now().timestamp(),
        };

        Ok(vec![Envelope::new(
            format!("http_event_{id}"),
            Box::new(stored_event),
            Default::default(),
        )])
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 6. MAIN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Torii HTTP-Only Sink Example\n");
    println!("This sink demonstrates:");
    println!("  ✓ In-memory storage");
    println!("  ✓ Custom HTTP REST endpoints");
    println!("  ✓ No EventBus publishing");
    println!("  ✓ No gRPC service\n");

    // Create sink and decoder
    let http_sink = HttpSink::new();
    let http_decoder = Arc::new(HttpDecoder::new());

    // Get sample events
    let sample_events = HttpSink::generate_sample_events();
    println!("📋 Loaded {} sample events\n", sample_events.len());

    // Configure Torii
    let config = ToriiConfig::builder()
        .port(8080)
        .host("0.0.0.0".to_string())
        .add_sink_boxed(Box::new(http_sink))
        .add_decoder(http_decoder)
        .with_sample_events(sample_events)
        .cycle_interval(3)
        .events_per_cycle(1)
        .build();

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📦 HTTP-ONLY SINK CONFIGURATION");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🌐 Server: http://0.0.0.0:8080");
    println!("🔄 ETL Cycle: {} seconds", config.cycle_interval);
    println!("📊 Events/Cycle: {}", config.events_per_cycle);
    println!();
    println!("📍 Topics: None (HTTP only)");
    println!("📡 gRPC: None (HTTP only)");
    println!("🔌 Storage: In-memory");
    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🧪 TESTING GUIDE");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();
    println!("📡 HTTP Endpoints:");
    println!("   # Get all events");
    println!("   curl http://localhost:8080/http/events");
    println!();
    println!("   # Get event count");
    println!("   curl http://localhost:8080/http/count");
    println!();
    println!("   # Clear all events");
    println!("   curl -X POST http://localhost:8080/http/clear");
    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🚀 Starting server...\n");

    run(config).await
}
