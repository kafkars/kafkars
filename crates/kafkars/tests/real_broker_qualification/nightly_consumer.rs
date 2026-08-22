//! Direct Fetch position replacement and missing-offset reset scenarios.

use std::io;

use kafkars::{OffsetReset, Record, StartPosition, TopicPartition};

use crate::real_broker_support::{OPERATION_TIMEOUT, TestError, wait_within};

use super::{consume, nightly_support};

pub(super) fn fetch_seek_and_offset_reset() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("direct-seek", 1)?;
    let producer = fixture.client.producer().build()?;
    for value in ["first", "second"] {
        wait_within(
            producer.send(Record::to(fixture.topic.as_str()).partition(0).value(value)),
            "direct seek seed delivery",
        )??;
    }

    let partition = TopicPartition::new(&fixture.topic, 0);
    let mut direct = fixture.client.assigned_consumer().build()?;
    consume::retry_assigned_control("direct seek initial assignment", || {
        direct.try_replace_assignment(
            [partition.clone().start_at(StartPosition::Beginning)],
            OPERATION_TIMEOUT,
        )
    })?;
    require_direct_value(&mut direct, b"first")?;
    consume::retry_assigned_control("direct seek to beginning", || {
        direct.try_seek(&partition, StartPosition::Beginning, OPERATION_TIMEOUT)
    })?;
    require_direct_value(&mut direct, b"first")?;
    consume::retry_assigned_control("direct seek to end", || {
        direct.try_seek(&partition, StartPosition::End, OPERATION_TIMEOUT)
    })?;
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("after-end"),
        ),
        "post-seek delivery",
    )??;
    require_direct_value(&mut direct, b"after-end")?;
    consume::close_assigned(&mut direct, "direct seek consumer close")?;

    let group_id = crate::real_broker_support::unique_name("kafkars-offset-reset");
    let mut group = consume::build_group(
        fixture
            .client
            .consumer(group_id)
            .subscribe([&fixture.topic])
            .on_missing_offset(OffsetReset::Earliest),
        "offset-reset member build",
    )?;
    let batch = wait_within(group.recv(), "offset-reset group receive")??
        .ok_or_else(|| io::Error::other("offset-reset group closed before delivery"))?;
    if !batch
        .records()
        .any(|record| record.value() == Some(b"first".as_slice()))
    {
        return Err(io::Error::other("earliest reset omitted the first record").into());
    }
    drop(batch);
    consume::close_group(group, "offset-reset group close")?;
    nightly_support::close_producer(&producer, "direct seek producer close")?;
    fixture.finish()
}

fn require_direct_value(
    consumer: &mut kafkars::AssignedConsumer,
    expected: &[u8],
) -> Result<(), TestError> {
    let batch = wait_within(consumer.recv(), "direct seek receive")??
        .ok_or_else(|| io::Error::other("direct consumer closed before delivery"))?;
    if batch
        .records()
        .any(|record| record.value() == Some(expected))
    {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "direct consumer batch omitted {:?}",
        String::from_utf8_lossy(expected)
    ))
    .into())
}
