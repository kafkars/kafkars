//! Compile-checked transactional-owner initialization.

use kafkars::{Client, KafkaError};

fn main() {}

#[allow(dead_code)]
async fn initialize_transactional_owner() -> Result<(), KafkaError> {
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .build()?;
    let producer = client
        .transactional_producer("invoice-worker-v1")
        .build()
        .await?;
    let _identity = producer.identity();
    producer.close();
    Ok(())
}
