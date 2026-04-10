use torii_pathfinder::{connect, EventFetcher};

const DB_PATH: &str = "/mnt/store/mainnet.sqlite";

const BATCH_SIZE: u64 = 10000;

fn main() {
    let conn = connect(DB_PATH).unwrap();
    let mut current_block = 6000000;
    for _ in 0..100 {
        let (blocks, events) = conn
            .get_events_with_context(current_block, current_block + BATCH_SIZE - 1)
            .expect("failed to fetch events with context");
        println!(
            "Fetched {} blocks and {} events for blocks {} to {}",
            blocks.len(),
            events.len(),
            current_block,
            current_block + BATCH_SIZE - 1
        );
        current_block += BATCH_SIZE;
    }
}
