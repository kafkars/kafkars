//! Compile-checked transactional-producer API sketch.

use kafka_client::{Client, KafkaError};

fn main() {}

#[allow(dead_code)]
async fn transact() -> Result<(), KafkaError> {
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .build()?;
    let mut producer = client
        .transactional_producer("invoice-worker-v1")
        .build()
        .await?;
    let transaction = producer.begin().await?;
    transaction.commit().await
}
