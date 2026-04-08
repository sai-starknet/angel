use anyhow::Result;
use async_trait::async_trait;
use starknet_types_raw::Felt;
use std::any::Any;
use torii::etl::envelope::{Envelope, TypeId, TypedBody};
use torii::etl::{Decoder, EventContext};

/// Decoded log entry
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub message: String,
    pub block_number: u64,
    pub event_key: String,
}

impl TypedBody for LogEntry {
    fn envelope_type_id(&self) -> TypeId {
        TypeId::new("log.entry")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Decoder that converts events into log entries.
pub struct LogDecoder {
    /// Optional event key filter (e.g., only decode events with specific keys).
    key_filter: Option<String>,
}

impl LogDecoder {
    /// Creates a new LogDecoder.
    ///
    /// If `key_filter` is Some, only events with keys containing this string will be decoded.
    pub fn new(key_filter: Option<String>) -> Self {
        Self { key_filter }
    }
}

#[async_trait]
impl Decoder for LogDecoder {
    fn decoder_name(&self) -> &'static str {
        "log"
    }

    async fn decode(
        &self,
        keys: &[Felt],
        data: &[Felt],
        context: EventContext,
    ) -> Result<Vec<Envelope>> {
        // Apply key filter if specified
        if let Some(ref filter) = self.key_filter {
            let matches = keys.iter().any(|k| {
                let key_hex = format!("{k:#x}");
                key_hex.contains(filter)
            });
            if !matches {
                return Ok(Vec::new());
            }
        }

        // Extract log message from event data.
        // For this example, we'll convert the first data field to a string representation.
        let message = if data.is_empty() {
            "Empty event data".to_string()
        } else {
            format!("Event data: {:#x}", data[0])
        };

        let event_key = if keys.is_empty() {
            "no-key".to_string()
        } else {
            format!("{:#x}", keys[0])
        };

        let log_entry = LogEntry {
            message,
            block_number: context.block_number,
            event_key,
        };

        // Create envelope with unique ID
        let envelope_id = format!(
            "log_{}_{}",
            context.block_number,
            format!("{:#x}", context.transaction_hash)
        );
        let envelope = Envelope::new(
            envelope_id,
            Box::new(log_entry),
            std::collections::HashMap::new(),
        );

        Ok(vec![envelope])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use starknet_types_raw::Felt;
    use torii::etl::StarknetEvent;

    #[tokio::test]
    async fn test_decode_logs() {
        let decoder = LogDecoder::new(None);

        let event = StarknetEvent {
            from_address: Felt::from(1u64),
            keys: vec![Felt::from(0x1234u64)],
            data: vec![Felt::from(0x5678u64)],
            block_number: 100,
            transaction_hash: Felt::from(0xabcdu64),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].type_id, TypeId::new("log.entry"));
    }
}
