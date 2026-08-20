//! Compile-checked group-consumer API sketch.

use kafkars::Client;

fn main() {}

#[allow(dead_code)]
async fn consume() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .build()?;
    let mut consumer = client
        .consumer("invoice-workers")
        .subscribe(["invoice-created"])
        .build()?;

    while let Some(batch) = consumer.recv().await? {
        for record in batch.records() {
            let _value = record.value();
        }
        let _checkpoint = batch.checkpoint();
    }

    client.shutdown().await?;
    Ok(())
}
