use async_trait::async_trait;
use axum::body::Body;
use axum::http::Request;
use axum::Router;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use starknet::macros::selector;
use starknet_types_raw::Felt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tower::ServiceExt;

use torii::etl::decoder::{ContractFilter, DecoderContext, DecoderId};
use torii::etl::engine_db::{EngineDb, EngineDbConfig};
use torii::etl::envelope::{Envelope, TypeId, TypedBody};
use torii::etl::extractor::{ExtractionBatch, RetryPolicy};
use torii::etl::sink::{EventBus, MultiSink, Sink, SinkContext, TopicInfo};
use torii::etl::{Decoder, StarknetEvent};
use torii::grpc::proto::TopicSubscription;
use torii::grpc::SubscriptionManager;
use torii::http::create_http_router;
use torii_common::{blob_to_felt, blob_to_u256, felt_to_blob, u256_to_blob};
use torii_erc1155::decoder::Erc1155Decoder;
use torii_erc20::decoder::Erc20Decoder;
use torii_erc20::storage::{Erc20Storage, TransferData, TransferDirection};
use torii_erc721::decoder::Erc721Decoder;

const TRANSFER_SELECTOR: Felt = Felt::selector("Transfer");

#[derive(Debug)]
struct BenchBody {
    value: u64,
}

impl TypedBody for BenchBody {
    fn envelope_type_id(&self) -> TypeId {
        TypeId::new("bench.body")
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct MockDecoder {
    name: &'static str,
    contract: Felt,
}

#[async_trait]
impl Decoder for MockDecoder {
    fn decoder_name(&self) -> &str {
        self.name
    }

    async fn decode(
        &self,
        from_address: Felt,
        keys: &[Felt],
        data: &[Felt],
        block_number: u64,
        transaction_hash: Felt,
    ) -> anyhow::Result<Vec<Envelope>> {
        if from_address != self.contract {
            return Ok(Vec::new());
        }

        Ok(vec![Envelope::new(
            format!("{}:{}", self.name, block_number),
            Box::new(BenchBody {
                value: block_number,
            }),
            HashMap::new(),
        )])
    }
}

struct MockSink {
    name: String,
}

#[async_trait]
impl Sink for MockSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("bench.body")]
    }

    async fn process(
        &self,
        envelopes: &[Envelope],
        _batch: &ExtractionBatch,
    ) -> anyhow::Result<()> {
        for envelope in envelopes {
            let _ = envelope.downcast_ref::<BenchBody>();
        }
        Ok(())
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![TopicInfo::new("bench.topic", Vec::new(), "Bench topic")]
    }

    fn build_routes(&self) -> Router {
        Router::new()
    }

    async fn initialize(
        &mut self,
        _event_bus: Arc<EventBus>,
        _context: &SinkContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime")
}

fn make_engine_db(rt: &Runtime) -> EngineDb {
    rt.block_on(async {
        EngineDb::new(EngineDbConfig {
            path: "sqlite::memory:".to_string(),
        })
        .await
        .expect("failed to create engine db")
    })
}

fn make_erc20_transfer_event(seed: u64) -> StarknetEvent {
    StarknetEvent {
        from_address: Felt::from(0x1000 + seed),
        keys: vec![
            TRANSFER_SELECTOR,
            Felt::from(0x2000 + seed),
            Felt::from(0x3000 + seed),
        ],
        data: vec![Felt::from(10_000 + seed), Felt::ZERO],
        block_number: 1_000_000 + seed,
        transaction_hash: Felt::from(0x4000 + seed),
    }
}

fn make_erc721_transfer_event(seed: u64) -> StarknetEvent {
    StarknetEvent {
        from_address: Felt::from(0x1100 + seed),
        keys: vec![
            TRANSFER_SELECTOR,
            Felt::from(0x2200 + seed),
            Felt::from(0x3300 + seed),
            Felt::from(seed),
            Felt::ZERO,
        ],
        data: vec![],
        block_number: 2_000_000 + seed,
        transaction_hash: Felt::from(0x4400 + seed),
    }
}

fn make_erc1155_transfer_single_event(seed: u64) -> StarknetEvent {
    StarknetEvent {
        from_address: Felt::from(0x1200 + seed),
        keys: vec![
            TRANSFER_SELECTOR,
            Felt::from(0x2300 + seed),
            Felt::from(0x3400 + seed),
            Felt::from(0x4500 + seed),
        ],
        data: vec![
            Felt::from(seed),
            Felt::ZERO,
            Felt::from(100 + seed),
            Felt::ZERO,
        ],
        block_number: 3_000_000 + seed,
        transaction_hash: Felt::from(0x4600 + seed),
    }
}

fn make_context_event(contract: Felt, seed: u64) -> StarknetEvent {
    StarknetEvent {
        from_address: contract,
        keys: vec![TRANSFER_SELECTOR],
        data: Vec::new(),
        block_number: 10_000 + seed,
        transaction_hash: Felt::from(0x5000 + seed),
    }
}

fn make_transfer_batch(size: usize, offset: u64) -> Vec<TransferData> {
    (0..size)
        .map(|i| {
            let i = i as u64;
            TransferData {
                id: None,
                token: Felt::from(0x5000 + ((i + offset) % 4)),
                from: Felt::from(0x6000 + ((i + offset) % 1024)),
                to: Felt::from(0x7000 + ((i + offset + 7) % 1024)),
                amount: U256::from_words((i + offset) as u128, 0),
                block_number: 4_000_000 + i + offset,
                tx_hash: Felt::from(0x8000 + i + offset),
                timestamp: None,
            }
        })
        .collect()
}

fn make_block_timestamps(size: usize, offset: u64) -> HashMap<u64, u64> {
    let mut out = HashMap::with_capacity(size);
    for i in 0..size {
        let n = i as u64 + offset;
        out.insert(1_000_000 + n, 1_700_000_000 + n);
    }
    out
}

fn make_bench_envelopes(size: usize) -> Vec<Envelope> {
    (0..size)
        .map(|i| {
            Envelope::new(
                format!("bench-envelope-{i}"),
                Box::new(BenchBody { value: i as u64 }),
                HashMap::new(),
            )
        })
        .collect()
}

fn make_mock_sinks(count: usize) -> Vec<Arc<dyn Sink>> {
    (0..count)
        .map(|i| {
            Arc::new(MockSink {
                name: format!("sink-{i}"),
            }) as Arc<dyn Sink>
        })
        .collect()
}

fn benchmark_common_conversions(c: &mut Criterion) {
    let mut group = c.benchmark_group("common_conversions");

    let felt = Felt::from_hex_unchecked(
        "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );
    group.bench_function("felt_roundtrip", |b| {
        b.iter(|| {
            let blob = felt_to_blob(black_box(felt));
            black_box(blob_to_felt(black_box(&blob)))
        });
    });

    let small = U256::from(42u64);
    group.bench_function("u256_roundtrip_small", |b| {
        b.iter(|| {
            let blob = u256_to_blob(black_box(small));
            black_box(blob_to_u256(black_box(&blob)))
        });
    });

    let large = U256::from_words(u128::MAX - 7, u128::MAX - 11);
    group.bench_function("u256_roundtrip_large", |b| {
        b.iter(|| {
            let blob = u256_to_blob(black_box(large));
            black_box(blob_to_u256(black_box(&blob)))
        });
    });

    group.finish();
}

fn benchmark_decoders(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("decoder_throughput");

    let erc20_decoder = Erc20Decoder::new();
    let erc20_event = make_erc20_transfer_event(1);
    group.bench_function("erc20_transfer_decode", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                erc20_decoder
                    .decode_event(black_box(&erc20_event))
                    .await
                    .expect("erc20 decode failed"),
            )
        });
    });

    let erc721_decoder = Erc721Decoder::new();
    let erc721_event = make_erc721_transfer_event(1);
    group.bench_function("erc721_transfer_decode", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                erc721_decoder
                    .decode_event(black_box(&erc721_event))
                    .await
                    .expect("erc721 decode failed"),
            )
        });
    });

    let erc1155_decoder = Erc1155Decoder::new();
    let erc1155_event = make_erc1155_transfer_single_event(1);
    group.bench_function("erc1155_transfer_single_decode", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                erc1155_decoder
                    .decode_event(black_box(&erc1155_event))
                    .await
                    .expect("erc1155 decode failed"),
            )
        });
    });

    let batch_events: Vec<_> = (0..256).map(make_erc20_transfer_event).collect();
    group.throughput(Throughput::Elements(batch_events.len() as u64));
    group.bench_function("erc20_decode_batch_256", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                erc20_decoder
                    .decode_events(black_box(&batch_events))
                    .await
                    .expect("erc20 batch decode failed"),
            )
        });
    });

    group.finish();
}

fn benchmark_decoder_context(c: &mut Criterion) {
    const DECODER_NAMES: [&str; 8] = [
        "bench-decoder-0",
        "bench-decoder-1",
        "bench-decoder-2",
        "bench-decoder-3",
        "bench-decoder-4",
        "bench-decoder-5",
        "bench-decoder-6",
        "bench-decoder-7",
    ];

    let rt = runtime();
    let mut group = c.benchmark_group("decoder_context");

    let mut decoders: Vec<Arc<dyn Decoder>> = Vec::new();
    for (i, name) in DECODER_NAMES.iter().enumerate() {
        let contract = if i == 6 {
            Felt::from(0xDEAD_u64)
        } else {
            Felt::from(0x9000_u64 + i as u64)
        };

        decoders.push(Arc::new(MockDecoder { name, contract }));
    }

    let engine_db = Arc::new(make_engine_db(&rt));
    let explicit_contract = Felt::from(0x9003_u64);
    let explicit_filter = ContractFilter::new()
        .map_contract(explicit_contract, vec![DecoderId::new("bench-decoder-3")]);

    let context_mapped = DecoderContext::new(decoders.clone(), engine_db.clone(), explicit_filter);
    let context_fallback =
        DecoderContext::new(decoders.clone(), engine_db.clone(), ContractFilter::new());

    let registry_cache = Arc::new(RwLock::new(HashMap::new()));
    rt.block_on(async {
        registry_cache.write().await.insert(
            Felt::from(0xDEAD_u64),
            vec![DecoderId::new("bench-decoder-6")],
        );
    });
    let context_registry =
        DecoderContext::with_registry(decoders, engine_db, ContractFilter::new(), registry_cache);

    let explicit_event = make_context_event(explicit_contract, 1);
    let fallback_event = make_context_event(Felt::from(0x9007_u64), 2);
    let registry_event = make_context_event(Felt::from(0xDEAD_u64), 3);

    group.bench_function("decode_event_explicit_mapping", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                context_mapped
                    .decode_event(black_box(&explicit_event))
                    .await
                    .expect("context explicit decode failed"),
            )
        });
    });

    group.bench_function("decode_event_fallback_all_decoders", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                context_fallback
                    .decode_event(black_box(&fallback_event))
                    .await
                    .expect("context fallback decode failed"),
            )
        });
    });

    group.bench_function("decode_event_registry_mapping", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                context_registry
                    .decode_event(black_box(&registry_event))
                    .await
                    .expect("context registry decode failed"),
            )
        });
    });

    let batch_events: Vec<_> = (0..256)
        .map(|i| {
            let contract = if i % 3 == 0 {
                explicit_contract
            } else if i % 3 == 1 {
                Felt::from(0x9007_u64)
            } else {
                Felt::from(0xDEAD_u64)
            };
            make_context_event(contract, i as u64)
        })
        .collect();

    group.throughput(Throughput::Elements(batch_events.len() as u64));
    group.bench_function("decode_batch_mixed_256", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                context_registry
                    .decode_events(black_box(&batch_events))
                    .await
                    .expect("context batch decode failed"),
            )
        });
    });

    group.finish();
}

fn benchmark_envelope_core(c: &mut Criterion) {
    let mut group = c.benchmark_group("etl_envelope");

    group.bench_function("envelope_create", |b| {
        b.iter(|| {
            let envelope = Envelope::new(
                black_box("bench-envelope".to_string()),
                Box::new(BenchBody { value: 7 }),
                HashMap::new(),
            );
            black_box(envelope)
        });
    });

    let envelope = Envelope::new(
        "bench-envelope".to_string(),
        Box::new(BenchBody { value: 99 }),
        HashMap::new(),
    );
    group.bench_function("envelope_downcast", |b| {
        b.iter(|| {
            let body = envelope
                .downcast_ref::<BenchBody>()
                .expect("downcast failed");
            black_box(body.value)
        });
    });

    group.finish();
}

fn benchmark_subscription_manager(c: &mut Criterion) {
    let mut group = c.benchmark_group("grpc_subscription_manager");

    let manager = Arc::new(SubscriptionManager::new());
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
    manager.register_client("bench-client".to_string(), tx);

    let topics_1 = vec![TopicSubscription {
        topic: "erc20.transfers".to_string(),
        filters: HashMap::from([("token".to_string(), "0x123".to_string())]),
        filter_data: None,
    }];

    let topics_3 = vec![
        TopicSubscription {
            topic: "erc20.transfers".to_string(),
            filters: HashMap::new(),
            filter_data: None,
        },
        TopicSubscription {
            topic: "erc721.transfers".to_string(),
            filters: HashMap::new(),
            filter_data: None,
        },
        TopicSubscription {
            topic: "erc1155.transfers".to_string(),
            filters: HashMap::new(),
            filter_data: None,
        },
    ];

    group.bench_function("update_subscriptions_1_topic", |b| {
        b.iter(|| {
            manager.update_subscriptions(
                black_box("bench-client"),
                black_box(topics_1.clone()),
                black_box(Vec::new()),
            );
        });
    });

    group.bench_function("update_subscriptions_3_topics", |b| {
        b.iter(|| {
            manager.update_subscriptions(
                black_box("bench-client"),
                black_box(topics_3.clone()),
                black_box(vec!["erc20.transfers".to_string()]),
            );
        });
    });

    group.finish();
}

fn benchmark_sink_fanout(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("sink_fanout");

    let envelopes = make_bench_envelopes(256);
    let batch = ExtractionBatch::empty();

    for sink_count in [1_usize, 4_usize, 8_usize] {
        let multi_sink = MultiSink::new(make_mock_sinks(sink_count));
        group.throughput(Throughput::Elements((sink_count * envelopes.len()) as u64));
        group.bench_with_input(
            BenchmarkId::new("multi_sink_process", sink_count),
            &sink_count,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let _: () = multi_sink
                        .process(black_box(&envelopes), black_box(&batch))
                        .await
                        .expect("multi sink process failed");
                    black_box(());
                });
            },
        );
    }

    group.finish();
}

fn benchmark_http_core(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("http_core");

    group.bench_function("create_http_router", |b| {
        b.iter(|| black_box(create_http_router()));
    });

    let app = create_http_router();
    group.bench_function("health_endpoint_oneshot", |b| {
        b.to_async(&rt).iter(|| async {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/health")
                        .body(Body::empty())
                        .expect("failed to build request"),
                )
                .await
                .expect("health endpoint failed");
            black_box(response.status())
        });
    });

    group.finish();
}

fn benchmark_retry_policy(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("extractor_retry_policy");

    let no_retry = RetryPolicy::no_retry();
    group.bench_function("execute_success_no_retry", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                no_retry
                    .execute(|| async { Ok::<u64, anyhow::Error>(42) })
                    .await
                    .expect("retry policy execute failed"),
            )
        });
    });

    let aggressive = RetryPolicy::aggressive();
    group.bench_function("execute_success_aggressive", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                aggressive
                    .execute(|| async { Ok::<u64, anyhow::Error>(42) })
                    .await
                    .expect("retry policy execute failed"),
            )
        });
    });

    group.finish();
}

fn benchmark_engine_db(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("engine_db_core");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(4));

    for size in [32_usize, 128_usize, 512_usize, 2_048_usize] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("insert_block_timestamps", size),
            &size,
            |b, &s| {
                b.iter_custom(|iters| {
                    let mut dbs = Vec::with_capacity(iters as usize);
                    for _ in 0..iters {
                        dbs.push(make_engine_db(&rt));
                    }

                    let timestamps = make_block_timestamps(s, 0);
                    let start = Instant::now();
                    for db in &dbs {
                        rt.block_on(db.insert_block_timestamps(&timestamps))
                            .expect("insert_block_timestamps failed");
                    }
                    start.elapsed()
                });
            },
        );
    }

    let db = make_engine_db(&rt);
    let preload = make_block_timestamps(20_000, 100_000);
    rt.block_on(db.insert_block_timestamps(&preload))
        .expect("engine db preload failed");

    let query_blocks: Vec<u64> = (0..128)
        .map(|i| {
            if i % 4 == 0 {
                9_000_000 + i as u64
            } else {
                1_100_000 + i as u64
            }
        })
        .collect();

    group.throughput(Throughput::Elements(query_blocks.len() as u64));
    group.bench_function("get_block_timestamps_128", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                db.get_block_timestamps(black_box(&query_blocks))
                    .await
                    .expect("get_block_timestamps failed"),
            )
        });
    });

    group.bench_function("get_block_timestamp_single", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                db.get_block_timestamp(black_box(1_100_001))
                    .await
                    .expect("get_block_timestamp failed"),
            )
        });
    });

    group.bench_function("set_extractor_state_upsert", |b| {
        b.to_async(&rt).iter(|| async {
            db.set_extractor_state("bench_extractor", "cursor", "12345")
                .await
                .expect("set_extractor_state failed");
            black_box(());
        });
    });

    group.bench_function("get_extractor_state", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                db.get_extractor_state("bench_extractor", "cursor")
                    .await
                    .expect("get_extractor_state failed"),
            )
        });
    });

    let contract = Felt::from(0xABCD_u64);
    let decoder_ids = vec![
        DecoderId::new("bench-decoder-1"),
        DecoderId::new("bench-decoder-2"),
        DecoderId::new("bench-decoder-3"),
    ];
    rt.block_on(db.set_contract_decoders(contract, &decoder_ids))
        .expect("seed contract decoders failed");

    group.bench_function("set_contract_decoders", |b| {
        b.to_async(&rt).iter(|| async {
            db.set_contract_decoders(contract, &decoder_ids)
                .await
                .expect("set_contract_decoders failed");
            black_box(());
        });
    });

    group.bench_function("get_contract_decoders", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                db.get_contract_decoders(contract)
                    .await
                    .expect("get_contract_decoders failed"),
            )
        });
    });

    group.finish();
}

fn benchmark_erc20_storage(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("erc20_storage");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(6));

    for size in [100usize, 1_000usize, 5_000usize] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("insert_transfers_batch", size),
            &size,
            |b, &s| {
                b.iter_custom(|iters| {
                    let mut fixtures = Vec::with_capacity(iters as usize);
                    for _ in 0..iters {
                        let dir = TempDir::new().expect("failed to create tempdir");
                        let db_path = dir.path().join("erc20_insert_bench.db");
                        let storage = rt
                            .block_on(Erc20Storage::new(
                                db_path.to_str().expect("invalid db path"),
                            ))
                            .expect("failed to create erc20 storage");
                        fixtures.push((dir, storage));
                    }

                    let batch = make_transfer_batch(s, 0);
                    let start = Instant::now();
                    for (_dir, storage) in fixtures {
                        black_box(
                            rt.block_on(storage.insert_transfers_batch(black_box(&batch)))
                                .expect("insert batch failed"),
                        );
                    }

                    start.elapsed()
                });
            },
        );
    }

    let dir = TempDir::new().expect("failed to create tempdir");
    let db_path = dir.path().join("erc20_query_bench.db");
    let storage = rt
        .block_on(Erc20Storage::new(
            db_path.to_str().expect("invalid db path"),
        ))
        .expect("failed to create erc20 storage");

    let preload = make_transfer_batch(20_000, 100_000);
    rt.block_on(storage.insert_transfers_batch(&preload))
        .expect("preload insert failed");

    let wallet = Felt::from(0x6000 + 7);
    group.bench_function("get_transfers_filtered_wallet", |b| {
        b.iter(|| {
            black_box(
                rt.block_on(storage.get_transfers_filtered(
                    Some(wallet),
                    None,
                    None,
                    &[],
                    TransferDirection::All,
                    Some(4_000_000),
                    None,
                    None,
                    100,
                ))
                .expect("wallet query failed"),
            )
        });
    });

    let token = Felt::from(0x5000 + 1);
    group.bench_function("get_transfers_filtered_token", |b| {
        b.iter(|| {
            black_box(
                rt.block_on(storage.get_transfers_filtered(
                    None,
                    None,
                    None,
                    &[token],
                    TransferDirection::All,
                    Some(4_000_000),
                    None,
                    None,
                    100,
                ))
                .expect("token query failed"),
            )
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_common_conversions,
    benchmark_decoders,
    benchmark_decoder_context,
    benchmark_envelope_core,
    benchmark_subscription_manager,
    benchmark_sink_fanout,
    benchmark_http_core,
    benchmark_retry_policy,
    benchmark_engine_db,
    benchmark_erc20_storage,
);
criterion_main!(benches);
