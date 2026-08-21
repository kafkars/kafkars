//! Compile-checked batched admin API sketch.

use kafkars::{Client, KafkaError, NewPartitions, NewTopic};

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

#[allow(dead_code)]
async fn create_partitions() -> Result<(), KafkaError> {
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .build()?;
    let result = client
        .admin()
        .create_partitions([
            NewPartitions::new("orders", 48),
            NewPartitions::new("audit", 12),
        ])
        .validate_only(false)
        .submit()
        .await?;
    assert_eq!(result.entries().len(), 2);
    Ok(())
}

#[allow(dead_code)]
async fn list_visible_topics() -> Result<(), KafkaError> {
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .build()?;
    let result = client
        .admin()
        .list_topics()
        .include_internal(false)
        .submit()
        .await?;
    for (name, description) in result.entries() {
        if let Ok(description) = description {
            assert_eq!(name.as_str(), description.name());
        }
    }
    Ok(())
}
