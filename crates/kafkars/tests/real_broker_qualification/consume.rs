//! Direct and classic-group public consumption with exact payload validation.

use std::{
    io, thread,
    time::{Duration, Instant},
};

use kafkars::{
    AssignedConsumer, Checkpoint, Client, Consumer, ConsumerBuilder, KafkaError, OffsetReset,
    RetryAdvice, StartPosition, TopicPartition,
};

use crate::real_broker_support::{OPERATION_TIMEOUT, TestError, wait_within, wait_within_for};

pub(super) fn direct(client: &Client, topic: &str, expected: &[&[u8]]) -> Result<(), TestError> {
    let mut consumer = client.assigned_consumer().build()?;
    retry_assigned_control("direct consumer assignment", || {
        consumer.try_replace_assignment(
            [TopicPartition::new(topic, 0).start_at(StartPosition::Beginning)],
            OPERATION_TIMEOUT,
        )
    })?;
    let batch = wait_within(consumer.recv(), "direct consumer receive")??
        .ok_or_else(|| io::Error::other("direct consumer closed before a batch arrived"))?;
    let values = batch
        .records()
        .filter_map(|record| record.value().map(<[u8]>::to_vec))
        .collect::<Vec<_>>();
    require_values(&values, expected)?;
    drop(batch);
    close_assigned(&mut consumer, "direct consumer close")?;
    Ok(())
}

pub(super) fn retry_assigned_control(
    phase: &str,
    mut operation: impl FnMut() -> Result<(), KafkaError>,
) -> Result<(), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        match operation() {
            Ok(()) => return Ok(()),
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {
                if Instant::now() >= deadline {
                    return Err(io::Error::other(format!("{phase} remained backpressured")).into());
                }
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) fn close_assigned(
    consumer: &mut AssignedConsumer,
    phase: &str,
) -> Result<(), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        match consumer.try_close() {
            Ok(close) => return wait_within(close, phase)?.map_err(Into::into),
            Err(error) if error.retry_advice() == RetryAdvice::RetrySafe => {
                if Instant::now() >= deadline {
                    return Err(io::Error::other(format!(
                        "{phase} admission remained backpressured"
                    ))
                    .into());
                }
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) fn classic_group(
    client: &Client,
    topic: &str,
    group_id: &str,
    expected: &[&[u8]],
) -> Result<(), TestError> {
    let mut consumer = build_group(
        client
            .consumer(group_id)
            .subscribe([topic])
            .on_missing_offset(OffsetReset::Earliest),
        "classic group build",
    )?;
    let batch = wait_within(consumer.recv(), "classic group receive")??
        .ok_or_else(|| io::Error::other("classic group closed before a batch arrived"))?;
    let values = batch
        .records()
        .filter_map(|record| record.value().map(<[u8]>::to_vec))
        .collect::<Vec<_>>();
    require_values(&values, expected)?;
    commit_group(&mut consumer, batch.checkpoint(), "classic group commit")?;
    close_group(consumer, "classic group close")?;
    Ok(())
}

pub(super) fn build_group(
    mut builder: ConsumerBuilder,
    phase: &str,
) -> Result<Consumer, TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{phase} admission remained backpressured"),
            )
            .into());
        }
        match builder
            .membership_start_timeout(deadline.saturating_duration_since(now))
            .build()
        {
            Ok(consumer) => return Ok(consumer),
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                if error.retry_advice() != RetryAdvice::RetrySafe {
                    return Err(error.into());
                }
                builder = returned;
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

pub(super) fn commit_group(
    consumer: &mut Consumer,
    mut checkpoint: Checkpoint,
    phase: &str,
) -> Result<(), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{phase} admission remained backpressured"),
            )
            .into());
        }
        let remaining = deadline.saturating_duration_since(now);
        match consumer.try_commit(checkpoint, remaining) {
            Ok(commit) => return wait_within_for(commit, phase, remaining)?.map_err(Into::into),
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                if error.retry_advice() != RetryAdvice::RetrySafe {
                    return Err(error.into());
                }
                checkpoint = returned;
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

pub(super) fn close_group(mut consumer: Consumer, phase: &str) -> Result<(), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        match consumer.try_close() {
            Ok(close) => return wait_within(close, phase)?.map_err(Into::into),
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                if error.retry_advice() != RetryAdvice::RetrySafe {
                    return Err(error.into());
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::other(format!(
                        "{phase} admission remained backpressured"
                    ))
                    .into());
                }
                consumer = returned;
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
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
