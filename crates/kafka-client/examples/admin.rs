//! Compile-checked batched admin API sketch.

use kafka_client::{Client, KafkaError, NewTopic};

fn main() {}

#[allow(dead_code)]
async fn create_topic() -> Result<(), KafkaError> {
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .build()?;

    let result = client
        .admin()
        .create_topics([NewTopic::new("orders", 24)
            .replication_factor(3)
            .config("cleanup.policy", "compact")])
        .validate_only(false)
        .submit()
        .await?;

    assert_eq!(result.entries().len(), 1);
    Ok(())
}

#[allow(dead_code)]
async fn delete_topics() -> Result<(), KafkaError> {
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .build()?;
    let result = client
        .admin()
        .delete_topics(["orders", "audit"])
        .submit()
        .await?;
    assert_eq!(result.entries().len(), 2);
    Ok(())
}
