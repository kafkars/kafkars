//! Compile-checked transactional ownership and direct-consumer record transfer.

use std::time::Duration;

use kafkars::{AssignedConsumer, Client, KafkaError, Transaction, TransactionalProducer};

const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Copies one directly assigned batch through ordinary transactional sends.
///
/// Each source record keeps its consumer delivery lease through destination
/// admission or rejection. The target record preserves timestamp, key, value,
/// headers, nulls, and empty values while this example explicitly keeps the
/// source partition. A send or commit error attempts an explicit abort before
/// returning the original error.
#[allow(dead_code)]
async fn copy_one_batch(
    source: &mut AssignedConsumer,
    destination: &mut TransactionalProducer,
    target_topic: &str,
) -> Result<bool, KafkaError> {
    let Some(batch) = source.recv().await? else {
        return Ok(false);
    };
    let mut transaction = destination.begin()?;

    for source_record in batch.into_owned().into_records() {
        let partition = source_record.partition();
        let target_record = source_record.into_record(target_topic).partition(partition);
        let send = match transaction.send(target_record, OPERATION_TIMEOUT) {
            Ok(send) => send,
            Err(rejection) => {
                let (_record, error) = rejection.into_parts();
                return Err(abort_after_error(transaction, error).await);
            }
        };
        if let Err(error) = send.await {
            return Err(abort_after_error(transaction, error).await);
        }
    }

    match transaction.commit(OPERATION_TIMEOUT) {
        Ok(commit) => commit.await.map(|()| true),
        Err(rejection) => {
            let (transaction, error) = rejection.into_parts();
            Err(abort_after_error(transaction, error).await)
        }
    }
}

async fn abort_after_error(transaction: Transaction<'_>, error: KafkaError) -> KafkaError {
    match transaction.abort(OPERATION_TIMEOUT) {
        Ok(abort) => {
            let _outcome = abort.await;
        }
        Err(rejection) => {
            let (_transaction, _abort_error) = rejection.into_parts();
        }
    }
    error
}
