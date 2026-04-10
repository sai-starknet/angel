use crate::connect;
use crate::extractor::PathfinderExtractor;
use crate::fetcher::EventFetcher;

const DB_PATH: &str = "/mnt/store/mainnet.sqlite";

#[test]
#[ignore = "requires /mnt/store/mainnet.sqlite snapshot"]
fn test_emitted_events() {
    let mut extractor =
        PathfinderExtractor::new(DB_PATH, 2000, 0, 4000000).expect("failed to create extractor");
    for n in 0..10000 {
        let (blocks, events) = extractor.next_batch().expect("failed to fetch batch");
        println!(
            "Fetched {} blocks and {} events",
            blocks.len(),
            events.len()
        );
        if blocks.is_empty() || n >= 20 {
            break;
        }
    }
}

#[test]
#[ignore = "requires /mnt/store/mainnet.sqlite snapshot"]
fn test_get_emitted_events_with_context() {
    let conn = connect(DB_PATH).unwrap();
    let (blocks, events) = conn
        .get_events_with_context(3000000, 3000010)
        .expect("failed to fetch events with context");
    println!(
        "Fetched {} blocks and {} events",
        blocks.len(),
        events.len()
    );
    for block in blocks {
        println!(
            "Block {}: hash {} timestamp {}",
            block.number, block.hash, block.timestamp
        );
    }
    for event in events {
        println!("{event:?}");
    }
}
