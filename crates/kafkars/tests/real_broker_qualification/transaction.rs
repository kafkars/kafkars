//! Commit and abort qualification over one public transactional producer owner.

use std::{
    io, thread,
    time::{Duration, Instant},
};

use kafkars::{Client, KafkaError, Record, RetryAdvice, Transaction, TransactionalProducer};

use crate::real_broker_support::{OPERATION_TIMEOUT, TestError, wait_within, wait_within_for};

pub(super) fn commit_and_abort(client: &Client, topic: &str, id: &str) -> Result<(), TestError> {
    let mut producer = wait_within(
        client
            .transactional_producer(id)
            .transaction_timeout(Duration::from_secs(30))
            .deadline_after(OPERATION_TIMEOUT)
            .build(),
        "transactional producer initialization",
    )??;

    with_transaction(&mut producer, "transaction begin", |mut transaction| {
        let send = transaction.send(
            Record::to(topic)
                .partition(0)
                .value("transaction-committed"),
            OPERATION_TIMEOUT,
        )?;
        wait_within(send, "transactional committed send")??;
        commit(transaction, "transaction commit")
    })?;

    with_transaction(
        &mut producer,
        "transaction abort begin",
        |mut transaction| {
            let send = transaction.send(
                Record::to(topic).partition(0).value("transaction-aborted"),
                OPERATION_TIMEOUT,
            )?;
            wait_within(send, "transactional aborted send")??;
            abort(transaction, "transaction abort")
        },
    )?;
    producer.close();
    Ok(())
}

pub(super) fn with_transaction<T>(
    producer: &mut TransactionalProducer,
    phase: &str,
    action: impl for<'producer> FnOnce(Transaction<'producer>) -> Result<T, TestError>,
) -> Result<T, TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        match producer.begin() {
            Ok(transaction) => return action(transaction),
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {
                if Instant::now() >= deadline {
                    return Err(backpressure_timeout(phase));
                }
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) fn commit(transaction: Transaction<'_>, phase: &str) -> Result<(), TestError> {
    commit_result(transaction, phase)?.map_err(Into::into)
}

pub(super) fn commit_result(
    mut transaction: Transaction<'_>,
    phase: &str,
) -> Result<Result<(), KafkaError>, TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(backpressure_timeout(phase));
        }
        let remaining = deadline.saturating_duration_since(now);
        match transaction.commit(remaining) {
            Ok(observer) => {
                return wait_within_for(
                    observer,
                    phase,
                    deadline.saturating_duration_since(Instant::now()),
                )
                .map_err(Into::into);
            }
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                transaction = returned;
                if error.retry_advice() != RetryAdvice::RetrySafe {
                    return Ok(Err(error));
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

pub(super) fn abort(mut transaction: Transaction<'_>, phase: &str) -> Result<(), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(backpressure_timeout(phase));
        }
        let remaining = deadline.saturating_duration_since(now);
        match transaction.abort(remaining) {
            Ok(observer) => {
                return Ok(wait_within_for(
                    observer,
                    phase,
                    deadline.saturating_duration_since(Instant::now()),
                )??);
            }
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                transaction = returned;
                if error.retry_advice() != RetryAdvice::RetrySafe {
                    return Err(error.into());
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn backpressure_timeout(phase: &str) -> TestError {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!("{phase} admission remained backpressured"),
    )
    .into()
}
