// Example: EventBus-Only Sink
//
// This demonstrates the SIMPLEST possible sink pattern:
// - No storage/database
// - No HTTP routes
// - No gRPC service
// - ONLY publishes events to EventBus for real-time subscriptions
//
// Use case: Broadcasting events to subscribers without persistence
//
// Run: cargo run --example eventbus_only_sink

use anyhow::Result;
use axum::Router;
use prost::Message;
use prost_types::Any;
use starknet_types_raw::Felt;
use std::sync::Arc;
use torii::etl::envelope::{Envelope, TypeId, TypedBody};
use torii::etl::extractor::ExtractionBatch;
use torii::etl::sink::{EventBus, Sink, SinkContext, TopicInfo};
use torii::etl::{Decoder, EventContext};
use torii::{async_trait, run, ToriiConfig, UpdateType};
use torii_types::event::StarknetEvent;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 1. DEFINE EVENT TYPE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Clone, Message)]
pub struct BroadcastEvent {
    #[prost(string, tag = "1")]
    pub event_id: String,
    #[prost(string, tag = "2")]
    pub from_address: String,
    #[prost(uint64, tag = "3")]
    pub block_number: u64,
}

// TypedBody implementation to make it work with Envelope
impl TypedBody for BroadcastEvent {
    fn envelope_type_id(&self) -> TypeId {
        TypeId::new("broadcast.event")
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 2. EVENTBUS-ONLY SINK (No storage, no HTTP routes)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

pub struct BroadcastSink {
    event_bus: Option<Arc<EventBus>>,
}

impl Default for BroadcastSink {
    fn default() -> Self {
        Self::new()
    }
}

impl BroadcastSink {
    pub fn new() -> Self {
        Self { event_bus: None }
    }

    pub fn generate_sample_events() -> Vec<StarknetEvent> {
        vec![
            StarknetEvent {
                from_address: Felt::from_hex("0x1234567890abcdef").unwrap(),
                keys: vec![Felt::from_hex("0x1").unwrap()],
                data: vec![Felt::from_hex("0x100").unwrap()],
                block_number: 0,
                transaction_hash: Felt::ZERO,
            },
            StarknetEvent {
                from_address: Felt::from_hex("0xfedcba0987654321").unwrap(),
                keys: vec![Felt::from_hex("0x2").unwrap()],
                data: vec![Felt::from_hex("0x200").unwrap()],
                block_number: 0,
                transaction_hash: Felt::ZERO,
            },
        ]
    }
}

#[async_trait]
impl Sink for BroadcastSink {
    fn name(&self) -> &'static str {
        "broadcast"
    }

    fn interested_types(&self) -> Vec<TypeId> {
        vec![TypeId::new("broadcast.event")]
    }

    fn topics(&self) -> Vec<TopicInfo> {
        vec![TopicInfo::new(
            "broadcast",
            vec![],
            "Real-time event broadcast (no filters)",
        )]
    }

    async fn initialize(&mut self, event_bus: Arc<EventBus>, _context: &SinkContext) -> Result<()> {
        self.event_bus = Some(event_bus);
        tracing::info!("BroadcastSink initialized (EventBus only, no storage)");
        Ok(())
    }

    async fn process(&self, envelopes: &[Envelope], _batch: &ExtractionBatch) -> Result<()> {
        let Some(event_bus) = &self.event_bus else {
            return Ok(());
        };

        for envelope in envelopes {
            if envelope.type_id != TypeId::new("broadcast.event") {
                continue;
            }

            if let Some(broadcast_event) = envelope.downcast_ref::<BroadcastEvent>() {
                // Encode as protobuf Any
                let mut buf = Vec::new();
                broadcast_event.encode(&mut buf)?;
                let any = Any {
                    type_url: "type.googleapis.com/torii.example.BroadcastEvent".to_string(),
                    value: buf,
                };

                // Publish to EventBus (no filters needed for broadcast)
                event_bus.publish_protobuf(
                    "broadcast",
                    &envelope.id,
                    &any,
                    broadcast_event,
                    UpdateType::Created,
                    |_event: &BroadcastEvent, _filters| true, // Accept all
                );

                tracing::info!(
                    "📡 Broadcast event {} from {} at block {}",
                    envelope.id,
                    broadcast_event.from_address,
                    broadcast_event.block_number
                );
            }
        }

        Ok(())
    }

    fn build_routes(&self) -> Router {
        // NO HTTP ROUTES - EventBus only!
        Router::new()
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 3. SIMPLE DECODER
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

pub struct BroadcastDecoder;

impl Default for BroadcastDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl BroadcastDecoder {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Decoder for BroadcastDecoder {
    fn decoder_name(&self) -> &'static str {
        "broadcast"
    }

    async fn decode(
        &self,
        _keys: &[Felt],
        _data: &[Felt],
        context: EventContext,
    ) -> Result<Vec<Envelope>> {
        let broadcast_event = BroadcastEvent {
            event_id: format!("{:#x}", context.transaction_hash),
            from_address: format!("{:#x}", context.from_address),
            block_number: context.block_number,
        };

        Ok(vec![Envelope::new(
            format!("broadcast_{:#x}", context.transaction_hash),
            Box::new(broadcast_event),
            Default::default(),
        )])
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 4. MAIN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Torii EventBus-Only Sink Example\n");
    println!("This sink demonstrates:");
    println!("  ✓ Real-time event broadcasting");
    println!("  ✓ No storage/database");
    println!("  ✓ No HTTP routes");
    println!("  ✓ EventBus subscriptions only\n");

    // Create sink and decoder
    let broadcast_sink = BroadcastSink::new();
    let broadcast_decoder = Arc::new(BroadcastDecoder::new());

    // Get sample events
    let sample_events = BroadcastSink::generate_sample_events();
    println!("📋 Loaded {} sample events\n", sample_events.len());

    // Configure Torii
    let config = ToriiConfig::builder()
        .port(8080)
        .host("0.0.0.0".to_string())
        .add_sink_boxed(Box::new(broadcast_sink))
        .add_decoder(broadcast_decoder)
        .with_sample_events(sample_events)
        .cycle_interval(3)
        .events_per_cycle(1)
        .build();

    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📦 EVENTBUS-ONLY SINK CONFIGURATION");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🌐 Server: http://0.0.0.0:8080");
    println!("🔄 ETL Cycle: {} seconds", config.cycle_interval);
    println!("📊 Events/Cycle: {}", config.events_per_cycle);
    println!();
    println!("📍 Topics: broadcast");
    println!("📡 HTTP Routes: None (EventBus only)");
    println!("🔌 Storage: None (streaming only)");
    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🧪 TESTING GUIDE");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();
    println!("📡 Subscribe to broadcast events:");
    println!(r#"   grpcurl -plaintext -d '{{"topics":["broadcast"]}}' \"#);
    println!("     localhost:8080 torii.Torii/Subscribe");
    println!();
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🚀 Starting server...\n");

    run(config).await
}
