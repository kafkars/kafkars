//! Compile-checked group-consumer API sketch.

use kafka_client::{Client, KafkaError, OffsetReset};

fn main() {}

#[allow(dead_code)]
async fn consume() -> Result<(), KafkaError> {
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .build()?;
    let mut consumer = client
        .consumer("invoice-workers")
        .subscribe(["invoice-created"])
        .on_missing_offset(OffsetReset::Earliest)
        .build()?;

    while let Some(batch) = consumer.next_batch().await? {
        for record in batch.records() {
            let _value = record.value();
        }
        let _next_offset = batch.checkpoint_next_offset();
    }

    client.shutdown().await
}
