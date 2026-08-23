//! Transaction fencing, abort, commit, and read-committed visibility.

use std::{
    io, thread,
    time::{Duration, Instant},
};

use kafkars::{ErrorKind, OffsetReset, ReadIsolation, Record, RetryAdvice};

use crate::real_broker_support::{
    OPERATION_TIMEOUT, TestError, unique_name, wait_within, wait_within_for,
};

use super::{consume, nightly_support, transaction};

pub(super) fn fencing_abort_commit_and_read_committed() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("transaction", 1)?;
    let transaction_id = unique_name("kafkars-fencing");
    let mut first = transactional(&fixture, &transaction_id)?;
    let mut replacement = transaction::with_transaction(
        &mut first,
        "first transaction begin",
        |mut first_transaction| {
            transaction::send(
                &mut first_transaction,
                Record::to(fixture.topic.as_str())
                    .partition(0)
                    .value("fenced"),
                "first transactional send",
            )?;

            let replacement = transactional(&fixture, &transaction_id)?;
            let fenced =
                match transaction::commit_result(first_transaction, "fenced transaction terminal")?
                {
                    Ok(()) => {
                        return Err(
                            io::Error::other("replacement producer did not fence first").into()
                        );
                    }
                    Err(error) => error,
                };
            if fenced.kind() != ErrorKind::Fenced {
                return Err(
                    io::Error::other(format!("first producer was not fenced: {fenced:?}")).into(),
                );
            }
            Ok(replacement)
        },
    )?;

    transaction::with_transaction(
        &mut replacement,
        "replacement commit begin",
        |mut committed| {
            transaction::send(
                &mut committed,
                Record::to(fixture.topic.as_str())
                    .partition(0)
                    .value("committed"),
                "replacement committed send",
            )?;
            transaction::commit(committed, "replacement commit")
        },
    )?;

    transaction::with_transaction(
        &mut replacement,
        "replacement abort begin",
        |mut aborted| {
            transaction::send(
                &mut aborted,
                Record::to(fixture.topic.as_str())
                    .partition(0)
                    .value("aborted"),
                "replacement aborted send",
            )?;
            transaction::abort(aborted, "replacement abort")
        },
    )?;

    transaction::with_transaction(
        &mut replacement,
        "replacement sentinel begin",
        |mut sentinel| {
            transaction::send(
                &mut sentinel,
                Record::to(fixture.topic.as_str())
                    .partition(0)
                    .value("sentinel"),
                "replacement sentinel send",
            )?;
            transaction::commit(sentinel, "replacement sentinel commit")
        },
    )?;

    assert_read_committed_visibility(&fixture)?;
    first.close();
    replacement.close();
    fixture.finish()
}

fn assert_read_committed_visibility(fixture: &nightly_support::Fixture) -> Result<(), TestError> {
    let mut consumer = consume::build_group(
        fixture
            .client
            .consumer(unique_name("kafkars-read-committed"))
            .subscribe([&fixture.topic])
            .on_missing_offset(OffsetReset::Earliest)
            .read_isolation(ReadIsolation::ReadCommitted),
        "read-committed member build",
    )?;
    let mut saw_committed = false;
    let mut saw_sentinel = false;
    for _ in 0..4 {
        let batch = wait_within(consumer.recv(), "read-committed receive")??
            .ok_or_else(|| io::Error::other("read-committed consumer closed before sentinel"))?;
        for record in batch.records() {
            let Some(value) = record.value() else {
                continue;
            };
            if value == b"fenced" || value == b"aborted" {
                return Err(io::Error::other(
                    "read_committed visibility included an uncommitted record",
                )
                .into());
            }
            saw_committed |= value == b"committed";
            saw_sentinel |= value == b"sentinel";
        }
        if saw_sentinel {
            break;
        }
    }
    if !saw_sentinel {
        return Err(io::Error::other(
            "read_committed did not reach its committed sentinel within four visible batches",
        )
        .into());
    }
    if !saw_committed {
        return Err(io::Error::other(
            "read_committed reached its sentinel without the original committed record",
        )
        .into());
    }
    consume::close_group(consumer, "read-committed close")?;
    Ok(())
}

fn transactional(
    fixture: &nightly_support::Fixture,
    id: &str,
) -> Result<kafkars::TransactionalProducer, TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "transaction initialization admission remained backpressured",
            )
            .into());
        }
        let remaining = deadline.saturating_duration_since(now);
        let result = wait_within_for(
            fixture
                .client
                .transactional_producer(id)
                .transaction_timeout(Duration::from_secs(30))
                .deadline_after(remaining)
                .build(),
            "nightly transactional producer initialization",
            remaining,
        )?;
        match result {
            Ok(producer) => return Ok(producer),
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
}
