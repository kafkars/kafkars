//! Bounded at-least-once group consumption with explicit commit and shutdown.

use std::time::Duration;

use kafkars::{
    Client,
    consumer::{GroupConsumerRecord, OffsetReset},
};

fn main() {}

#[allow(dead_code)]
async fn consume<F>(max_batches: usize, mut process: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: for<'record> FnMut(GroupConsumerRecord<'record>) -> Result<(), Box<dyn std::error::Error>>,
{
    let client = Client::builder()
        .bootstrap_servers(["localhost:9092"])
        .build()?;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        let mut consumer = client
            .consumer("invoice-workers")
            .subscribe(["invoice-created"])
            // Deliberately replay retained records when this group is new.
            // The library default instead refuses missing committed offsets.
            .on_missing_offset(OffsetReset::Earliest)
            .build()?;

        for _ in 0..max_batches {
            let Some(batch) = consumer.recv().await? else {
                break;
            };
            for record in batch.records() {
                // Return success only after the application's durable work.
                // A processing or commit failure can cause replay on restart;
                // the application must make those side effects idempotent.
                process(record)?;
            }
            let checkpoint = batch.checkpoint();
            // Persist progress, rather than discard the linear checkpoint.
            // Do not blindly retry an uncertain or stale-assignment failure.
            consumer
                .try_commit(checkpoint, Duration::from_secs(30))?
                .await?;
        }

        consumer.try_close()?.await?;
        Ok(())
    }
    .await;
    // Shutdown also runs after processing, registration, commit, or close
    // failure, and never replaces the original application error.
    let shutdown = client.shutdown().await.map_err(Into::into);
    result.and(shutdown)
}
