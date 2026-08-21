//! Transaction fencing, abort, commit, and read-committed visibility.

use std::{io, time::Duration};

use kafkars::{ErrorKind, OffsetReset, ReadIsolation, Record};

use crate::real_broker_support::{OPERATION_TIMEOUT, TestError, unique_name, wait_within};

use super::nightly_support;

pub(super) fn fencing_abort_commit_and_read_committed() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("transaction", 1)?;
    let transaction_id = unique_name("kafkars-fencing");
    let mut first = transactional(&fixture, &transaction_id)?;
    let mut first_transaction = first.begin()?;
    wait_within(
        first_transaction.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("fenced"),
            OPERATION_TIMEOUT,
        )?,
        "first transactional send",
    )??;

    let mut replacement = transactional(&fixture, &transaction_id)?;
    let first_commit = first_transaction
        .commit(OPERATION_TIMEOUT)
        .map_err(|error| {
            io::Error::other(format!(
                "fenced transaction commit admission: {}",
                error.error()
            ))
        })?;
    let fenced = match wait_within(first_commit, "fenced transaction terminal")? {
        Ok(()) => return Err(io::Error::other("replacement producer did not fence first").into()),
        Err(error) => error,
    };
    if fenced.kind() != ErrorKind::Fenced {
        return Err(io::Error::other(format!("first producer was not fenced: {fenced:?}")).into());
    }

    let mut committed = replacement.begin()?;
    wait_within(
        committed.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("committed"),
            OPERATION_TIMEOUT,
        )?,
        "replacement committed send",
    )??;
    wait_within(
        committed.commit(OPERATION_TIMEOUT).map_err(|error| {
            io::Error::other(format!("replacement commit admission: {}", error.error()))
        })?,
        "replacement commit",
    )??;

    let mut aborted = replacement.begin()?;
    wait_within(
        aborted.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("aborted"),
            OPERATION_TIMEOUT,
        )?,
        "replacement aborted send",
    )??;
    wait_within(
        aborted.abort(OPERATION_TIMEOUT).map_err(|error| {
            io::Error::other(format!("replacement abort admission: {}", error.error()))
        })?,
        "replacement abort",
    )??;

    let mut consumer = fixture
        .client
        .consumer(unique_name("kafkars-read-committed"))
        .subscribe([&fixture.topic])
        .on_missing_offset(OffsetReset::Earliest)
        .read_isolation(ReadIsolation::ReadCommitted)
        .membership_start_timeout(OPERATION_TIMEOUT)
        .build()?;
    let batch = wait_within(consumer.recv(), "read-committed receive")??
        .ok_or_else(|| io::Error::other("read-committed consumer closed before delivery"))?;
    let values = batch
        .records()
        .filter_map(|record| record.value().map(<[u8]>::to_vec))
        .collect::<Vec<_>>();
    if !values.iter().any(|value| value == b"committed")
        || values
            .iter()
            .any(|value| value == b"aborted" || value == b"fenced")
    {
        return Err(
            io::Error::other("read_committed visibility included an uncommitted record").into(),
        );
    }
    wait_within(consumer.try_close()?, "read-committed close")??;
    first.close();
    replacement.close();
    fixture.finish()
}

fn transactional(
    fixture: &nightly_support::Fixture,
    id: &str,
) -> Result<kafkars::TransactionalProducer, TestError> {
    Ok(wait_within(
        fixture
            .client
            .transactional_producer(id)
            .transaction_timeout(Duration::from_secs(30))
            .deadline_after(OPERATION_TIMEOUT)
            .build(),
        "nightly transactional producer initialization",
    )??)
}
