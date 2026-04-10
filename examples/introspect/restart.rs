use itertools::Itertools;
use starknet::core::types::Felt;
use torii_dojo::decoder::DojoDecoder;
use torii_dojo::store::DojoStoreTrait;
use torii_dojo::DojoToriiError;
use torii_introspect::events::{IntrospectBody, IntrospectMsg};
use torii_introspect_sql_sink::IntrospectDb;
use torii_sql::{DbPool, PoolConfig};
use torii_test_utils::{resolve_path_like, FakeProvider, MultiContractEventIterator};

// const DB_URL: &str = "postgres://torii:torii@localhost:5432/torii";
const DB_URL: &str = "sqlite://sqlite-data.db?mode=rwc";
const EVENT_PATHS: [&str; 2] = ["~/tc-tests/blob-arena/events", "~/tc-tests/pistols/events"];
const MODEL_CONTRACTS_PATH: &str = "~/tc-tests/model-contracts";
const BATCH_SIZE: usize = 2000;
const PISTOLS_ADDRESS: Felt =
    Felt::from_hex_unchecked("08b4838140a3cbd36ebe64d4b5aaf56a30cc3753c928a79338bf56c53f506c5");
const BLOB_ARENA_ADDRESS: Felt =
    Felt::from_hex_unchecked("2d26295d6c541d64740e1ae56abc079b82b22c35ab83985ef8bd15dc0f9edfb");

// const SCHEMA_MAP: [(Felt, &str); 2] = [
//     (PISTOLS_ADDRESS, "pistols"),
//     (BLOB_ARENA_ADDRESS, "blob_arena"),
// ];

const ADDRESSES: [Felt; 2] = [PISTOLS_ADDRESS, BLOB_ARENA_ADDRESS];

async fn run_events(
    events: &mut MultiContractEventIterator,
    provider: FakeProvider,
    pool: DbPool,
    end: Option<u64>,
    event_n: &mut u32,
    success: &mut u32,
) -> bool {
    println!("Starting event processing run");
    let decoder = DojoDecoder::new(pool.clone(), provider);
    let db = IntrospectDb::new(pool, ADDRESSES);
    decoder.initialize().await.unwrap();
    decoder.load_tables(&[]).await.unwrap();
    let errors = db.initialize_introspect_sql_sink().await.unwrap();
    if !errors.is_empty() {
        for err in errors {
            println!("Error loading table: {err}");
        }
        panic!("");
    }
    let mut running = true;
    let mut this_run = 0;
    while running {
        let mut msgs = Vec::with_capacity(BATCH_SIZE);
        for _ in 0..BATCH_SIZE {
            let Some(event) = events.next() else {
                running = false;
                break;
            };
            *event_n += 1;
            this_run += 1;
            match decoder.decode_raw_event(&event).await {
                Ok(IntrospectBody {
                    context: metadata,
                    msg: IntrospectMsg::CreateTable(mut msg),
                }) => {
                    msg.append_only = true;
                    msgs.push((IntrospectMsg::CreateTable(msg), metadata).into());
                }
                Ok(msg) => {
                    msgs.push(msg);
                }
                Err(DojoToriiError::UnknownDojoEventSelector(_)) => {
                    println!("Unknown event selector, skipping event");
                }
                Err(err) => {
                    println!("Failed to decode event: {err:?}");
                }
            };
        }

        let msgs_ref = msgs.iter().collect_vec();
        for res in db.process_messages(msgs_ref).await.unwrap() {
            match res {
                Err(err) => println!("Failed to process message: {err:?}"),
                Ok(()) => *success += 1,
            }
        }
        println!(
            "Processed batch of events, total events processed: {event_n}, successful: {success}"
        );
        if let Some(end) = end {
            if end <= this_run as u64 {
                println!("Reached end of event range, stopping");
                return true;
            }
        }
    }
    false
}

#[tokio::main]
async fn main() {
    let event_paths = EVENT_PATHS.map(resolve_path_like).to_vec();
    let provider = FakeProvider::new(resolve_path_like(MODEL_CONTRACTS_PATH));
    let mut event_iterator = MultiContractEventIterator::new(event_paths);
    let pool = PoolConfig::new(DB_URL.to_string())
        .max_connections(5)
        .connect_any()
        .await
        .unwrap();
    let mut event_n = 0;
    let mut success = 0;
    while run_events(
        &mut event_iterator,
        provider.clone(),
        pool.clone(),
        Some(20000),
        &mut event_n,
        &mut success,
    )
    .await
    {}
    println!("Finished processing events");
}
