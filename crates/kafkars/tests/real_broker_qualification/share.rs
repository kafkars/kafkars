//! Bounded exact-ownership helpers for share-group qualification scenarios.

use std::{io, thread, time::Instant};

use kafkars::{
    Client, Producer, Record, RetryAdvice, ShareAcknowledgement, ShareConsumer, ShareConsumerBatch,
    ShareConsumerBuilder, ShareConsumerFetchConfig, ShareDisposition,
};

use crate::real_broker_support::{OPERATION_TIMEOUT, TestError, wait_within, wait_within_for};

pub(super) fn build(client: &Client, group: &str, topic: &str) -> Result<ShareConsumer, TestError> {
    let builder = client
        .share_consumer(group)
        .subscribe([topic])
        .fetch_config(
            ShareConsumerFetchConfig::default()
                .with_max_records(1)
                .with_batch_size(1),
        );
    build_within(builder)
}

fn build_within(mut builder: ShareConsumerBuilder) -> Result<ShareConsumer, TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "share registration remained backpressured",
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
                thread::yield_now();
            }
        }
    }
}

pub(super) fn produce(producer: &Producer, topic: &str, value: &[u8]) -> Result<i64, TestError> {
    Ok(wait_within(
        producer.send(Record::to(topic).partition(0).value(value.to_vec())),
        "share producer delivery",
    )??
    .offset())
}

pub(super) fn receive_exact(
    consumer: &mut ShareConsumer,
    topic: &str,
    offset: i64,
    value: &[u8],
) -> Result<(ShareConsumerBatch, i16), TestError> {
    let batch = wait_within(consumer.recv(), "share consumer receive")??
        .ok_or_else(|| io::Error::other("share consumer closed before delivery"))?;
    if batch.len() != 1 {
        return Err(io::Error::other(format!(
            "share delivery contained {} records instead of one",
            batch.len()
        ))
        .into());
    }
    let record = batch
        .records()
        .next()
        .unwrap_or_else(|| unreachable!("one-record share batch has one record"));
    let matches = record.topic() == topic
        && record.partition() == 0
        && record.offset() == offset
        && record.value() == Some(value);
    let delivery_count = record.delivery_count();
    if !matches {
        return Err(io::Error::other(format!(
            "share delivery did not match topic={topic} partition=0 offset={offset} value={value:?}"
        ))
        .into());
    }
    Ok((batch, delivery_count))
}

pub(super) fn accept_exact(
    consumer: &mut ShareConsumer,
    observed: (ShareConsumerBatch, i16),
) -> Result<(), TestError> {
    acknowledge(
        consumer,
        observed.0,
        ShareDisposition::Accept,
        "share accept",
    )
}

pub(super) fn acknowledge(
    consumer: &mut ShareConsumer,
    batch: ShareConsumerBatch,
    disposition: ShareDisposition,
    phase: &str,
) -> Result<(), TestError> {
    let decision = batch
        .records()
        .next()
        .unwrap_or_else(|| unreachable!("validated share batch has one record"))
        .decision(disposition);
    let acknowledgement = batch.into_acknowledgement(vec![decision])?;
    acknowledge_within(consumer, acknowledgement, phase)
}

fn acknowledge_within(
    consumer: &mut ShareConsumer,
    mut acknowledgement: ShareAcknowledgement,
    phase: &str,
) -> Result<(), TestError> {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{phase} remained backpressured"),
            )
            .into());
        }
        let remaining = deadline.saturating_duration_since(now);
        let operation = match consumer.try_acknowledge(acknowledgement, remaining) {
            Ok(operation) => operation,
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                if error.retry_advice() != RetryAdvice::RetrySafe {
                    return Err(error.into());
                }
                acknowledgement = returned;
                thread::yield_now();
                continue;
            }
        };
        match wait_within_for(operation, phase, remaining)? {
            Ok(response) => {
                if response
                    .partitions()
                    .any(|partition| partition.broker_code().is_some())
                {
                    return Err(io::Error::other(format!(
                        "{phase} returned a partition broker error"
                    ))
                    .into());
                }
                return Ok(());
            }
            Err(error) => {
                let (returned, semantic, broker) = error.into_parts();
                if semantic.retry_advice() != RetryAdvice::RetrySafe || returned.is_none() {
                    return Err(io::Error::other(format!(
                        "{phase} failed: error={semantic:?} broker={broker:?}"
                    ))
                    .into());
                }
                acknowledgement = returned.unwrap_or_else(|| {
                    unreachable!("safe share acknowledgement retry retained ownership")
                });
                thread::yield_now();
            }
        }
    }
}

pub(super) fn close(mut consumer: ShareConsumer, phase: &str) -> Result<(), TestError> {
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
        match consumer.try_close() {
            Ok(close) => {
                return wait_within_for(close, phase, deadline.saturating_duration_since(now))?
                    .map_err(Into::into);
            }
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                if error.retry_advice() != RetryAdvice::RetrySafe {
                    return Err(error.into());
                }
                consumer = returned;
                thread::yield_now();
            }
        }
    }
}
