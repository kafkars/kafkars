//! Commit and abort qualification over one public transactional producer owner.

use std::{io, time::Duration};

use kafkars::{Client, Record};

use crate::real_broker_support::{OPERATION_TIMEOUT, TestError, wait_within};

pub(super) fn commit_and_abort(client: &Client, topic: &str, id: &str) -> Result<(), TestError> {
    let mut producer = wait_within(
        client
            .transactional_producer(id)
            .transaction_timeout(Duration::from_secs(30))
            .deadline_after(OPERATION_TIMEOUT)
            .build(),
        "transactional producer initialization",
    )??;

    let mut transaction = producer.begin()?;
    let send = transaction.send(
        Record::to(topic)
            .partition(0)
            .value("transaction-committed"),
        OPERATION_TIMEOUT,
    )?;
    wait_within(send, "transactional committed send")??;
    let commit = transaction.commit(OPERATION_TIMEOUT).map_err(|error| {
        io::Error::other(format!("transaction commit admission: {}", error.error()))
    })?;
    wait_within(commit, "transaction commit")??;

    let mut transaction = producer.begin()?;
    let send = transaction.send(
        Record::to(topic).partition(0).value("transaction-aborted"),
        OPERATION_TIMEOUT,
    )?;
    wait_within(send, "transactional aborted send")??;
    let abort = transaction.abort(OPERATION_TIMEOUT).map_err(|error| {
        io::Error::other(format!("transaction abort admission: {}", error.error()))
    })?;
    wait_within(abort, "transaction abort")??;
    producer.close();
    Ok(())
}
