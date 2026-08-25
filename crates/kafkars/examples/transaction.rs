//! Compile-checked transactional ownership and direct-consumer record transfer.

use std::{sync::Arc, time::Duration};

use kafkars::{
    AssignedConsumer, Client, ErrorKind, KafkaError, Transaction, TransactionalProducer,
};

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
/// Each fallible transfer keeps separate source evidence through the transaction
/// terminal while the outgoing record shares bytes without copying payloads or
/// header names. The target record preserves timestamp, key, value, headers,
/// nulls, and empty values while this example explicitly keeps the source
/// partition. A send or commit error attempts an explicit abort before returning
/// the original error.
#[allow(dead_code)]
async fn copy_one_batch(
    source: &mut AssignedConsumer,
    destination: &mut TransactionalProducer,
    target_topic: Arc<str>,
) -> Result<bool, KafkaError> {
    let Some(batch) = source.recv().await? else {
        return Ok(false);
    };
    let batch = batch.into_owned();
    let mut retained_sources = Vec::new();
    retained_sources
        .try_reserve_exact(batch.len())
        .map_err(|_| {
            KafkaError::new(
                ErrorKind::Internal,
                "retained source-record capacity allocation failed",
            )
        })?;
    let mut transaction = destination.begin()?;

    for source_record in batch.into_records() {
        let partition = source_record.partition();
        let (target_record, retained_source) =
            match source_record.try_into_record(Arc::clone(&target_topic)) {
                Ok(transferred) => transferred,
                Err(rejection) => {
                    let (_source_record, _target_topic) = rejection.into_parts();
                    let error = KafkaError::new(
                        ErrorKind::Internal,
                        "consumer record transfer allocation failed",
                    );
                    return Err(abort_after_error(transaction, error).await);
                }
            };
        let target_record = target_record.partition(partition);
        let send = match transaction.send(target_record, OPERATION_TIMEOUT) {
            Ok(send) => {
                retained_sources.push(retained_source);
                send
            }
            Err(rejection) => {
                retained_sources.push(retained_source);
                let (_record, error) = rejection.into_parts();
                return Err(abort_after_error(transaction, error).await);
            }
        };
        if let Err(error) = send.await {
            return Err(abort_after_error(transaction, error).await);
        }
    }

    let outcome = match transaction.commit(OPERATION_TIMEOUT) {
        Ok(commit) => commit.await.map(|()| true),
        Err(rejection) => {
            let (transaction, error) = rejection.into_parts();
            Err(abort_after_error(transaction, error).await)
        }
    };
    drop(retained_sources);
    outcome
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
