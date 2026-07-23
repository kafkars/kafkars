//! Compile-checked native producer API sketch.

use kafka_client::{Client, KafkaError, Record};

fn main() {}

#[allow(dead_code)]
async fn produce() -> Result<(), KafkaError> {
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .client_id("orders-api")
        .build()?;
    let producer = client.producer().build()?;

    let record = Record::to("orders")
        .partition(0)
        .key("order-42")
        .value("created")
        .header("traceparent", "00-example");
    let delivery = match producer.try_send(record) {
        Ok(delivery) => delivery,
        Err(rejection) => {
            let (_record, error) = rejection.into_parts();
            return Err(error);
        }
    };
    let metadata = delivery.await?;

    assert_eq!(metadata.topic(), "orders");
    client.shutdown().await
}
