//! Direct and classic-group public consumption with exact payload validation.

use std::io;

use kafkars::{Client, OffsetReset, StartPosition, TopicPartition};

use crate::real_broker_support::{OPERATION_TIMEOUT, TestError, wait_within};

pub(super) fn direct(client: &Client, topic: &str, expected: &[&[u8]]) -> Result<(), TestError> {
    let mut consumer = client.assigned_consumer().build()?;
    consumer.try_replace_assignment(
        [TopicPartition::new(topic, 0).start_at(StartPosition::Beginning)],
        OPERATION_TIMEOUT,
    )?;
    let batch = wait_within(consumer.recv(), "direct consumer receive")??
        .ok_or_else(|| io::Error::other("direct consumer closed before a batch arrived"))?;
    let values = batch
        .records()
        .filter_map(|record| record.value().map(<[u8]>::to_vec))
        .collect::<Vec<_>>();
    require_values(&values, expected)?;
    wait_within(consumer.try_close()?, "direct consumer close")??;
    Ok(())
}

pub(super) fn classic_group(
    client: &Client,
    topic: &str,
    group_id: &str,
    expected: &[&[u8]],
) -> Result<(), TestError> {
    let mut consumer = client
        .consumer(group_id)
        .subscribe([topic])
        .on_missing_offset(OffsetReset::Earliest)
        .membership_start_timeout(OPERATION_TIMEOUT)
        .build()?;
    let batch = wait_within(consumer.recv(), "classic group receive")??
        .ok_or_else(|| io::Error::other("classic group closed before a batch arrived"))?;
    let values = batch
        .records()
        .filter_map(|record| record.value().map(<[u8]>::to_vec))
        .collect::<Vec<_>>();
    require_values(&values, expected)?;
    let checkpoint = batch.checkpoint();
    let commit = consumer.try_commit(checkpoint, OPERATION_TIMEOUT)?;
    wait_within(commit, "classic group commit")??;
    let close = consumer.try_close()?;
    wait_within(close, "classic group close")??;
    Ok(())
}

fn require_values(actual: &[Vec<u8>], expected: &[&[u8]]) -> Result<(), TestError> {
    if expected
        .iter()
        .all(|value| actual.iter().any(|actual| actual.as_slice() == *value))
    {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "consumer batch omitted expected payloads; observed {} records",
        actual.len()
    ))
    .into())
}
