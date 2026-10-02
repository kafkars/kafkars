//! Native producer sketch with terminal shutdown after success or error.

use kafkars::{Client, Result, producer::Record};

fn main() {}

#[allow(dead_code)]
async fn produce() -> Result<()> {
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .client_id("orders-api")
        .build()?;
    let result: Result<()> = async {
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
        Ok(())
    }
    .await;
    // Observe terminal cleanup even after admission or delivery failure,
    // without replacing the original operation error.
    let shutdown = client.shutdown().await;
    result.and(shutdown)
}
