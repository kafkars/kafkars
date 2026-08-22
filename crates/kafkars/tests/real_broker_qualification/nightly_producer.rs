//! Real batching, partition placement, cancellation, and delivery-certainty scenarios.

use std::{
    io, thread,
    time::{Duration, Instant},
};

use kafkars::{CancellationOutcome, Delivery, DeliveryStatus, ErrorKind, ProducerLimits, Record};

use crate::real_broker_support::{TestError, client_builder_from_environment, wait_within};

use super::{nightly_control::BrokerGuard, nightly_support};

pub(super) fn batching_and_partitioning() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("producer-batch", 3)?;
    let producer = fixture.client.producer().build()?;
    let records = (0..18)
        .map(|index| {
            Record::to(fixture.topic.as_str())
                .partition(index % 3)
                .value(format!("batch-{index}"))
        })
        .collect::<Vec<_>>();
    let result = nightly_support::send_batch_after_transient_admission(
        &producer,
        records,
        "batched producer send",
    )?;
    if let Some(rejection) = result.rejection() {
        return Err(io::Error::other(format!(
            "batch admission accepted {} of 18 records before rejection: error={:?}; rejected={}",
            result.deliveries().len(),
            rejection.error(),
            rejection.record().len(),
        ))
        .into());
    }
    if result.deliveries().len() != 18 {
        return Err(io::Error::other(format!(
            "batch admission returned {} deliveries without a rejection",
            result.deliveries().len(),
        ))
        .into());
    }
    for (index, delivery) in result.deliveries().iter().enumerate() {
        let metadata = delivery.as_ref().map_err(Clone::clone)?;
        let expected_partition = i32::try_from(index % 3)?;
        if metadata.partition() != expected_partition {
            return Err(io::Error::other("producer changed explicit partition order").into());
        }
    }
    nightly_support::flush_producer(&producer, "batched producer flush")?;
    let metrics = wait_within(fixture.client.metrics()?, "producer batching metrics")??;
    let producer_metrics = metrics.producer();
    if producer_metrics.produce_records() < 18
        || producer_metrics.produce_batches() >= producer_metrics.produce_records()
    {
        return Err(io::Error::other("producer metrics did not prove record batching").into());
    }
    nightly_support::close_producer(&producer, "batched producer close")?;
    fixture.finish()
}

pub(super) fn cancellation_preserves_delivery_certainty() -> Result<(), TestError> {
    let delivery_timeout = Duration::from_secs(2);
    let limits = ProducerLimits::default().with_linger(Duration::from_millis(50));
    let builder = client_builder_from_environment("kafkars-nightly-cancellation")?
        .producer_limits(limits)
        .producer_delivery_timeout(delivery_timeout);
    let fixture = nightly_support::Fixture::from_builder(builder, "producer-cancel", 1)?;
    let producer = fixture.client.producer().build()?;
    wait_within(
        producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("warmup"),
        ),
        "producer cancellation warmup",
    )??;
    let topic = nightly_support::describe_topic(&fixture.client.admin(), &fixture.topic)?;
    let leader = topic.partitions()[0]
        .leader_id()
        .ok_or_else(|| io::Error::other("qualification topic has no leader"))?;

    let paused = BrokerGuard::pause(leader)?;
    let operation_deadline = Instant::now() + delivery_timeout;
    let mut delivery = producer.try_send(
        Record::to(fixture.topic.as_str())
            .partition(0)
            .value("cancellation-race"),
    )?;
    let cancellation = cancel_within(&mut delivery, operation_deadline)?;
    let terminal = wait_within(delivery, "cancellation-race producer terminal")?;
    paused.restore()?;
    let error = terminal
        .err()
        .ok_or_else(|| io::Error::other("paused broker unexpectedly acknowledged Produce"))?;
    let certainty_preserved = match cancellation {
        CancellationOutcome::CancelledNotSent => {
            error.kind() == ErrorKind::Cancelled
                && error.delivery_status() == Some(DeliveryStatus::NotSent)
        }
        CancellationOutcome::TooLate | CancellationOutcome::AlreadyTerminal => {
            matches!(error.kind(), ErrorKind::Timeout | ErrorKind::Transport)
                && matches!(
                    error.delivery_status(),
                    Some(DeliveryStatus::NotSent | DeliveryStatus::PossiblySent)
                )
        }
    };
    if !certainty_preserved {
        return Err(io::Error::other(format!(
            "cancellation did not preserve delivery certainty: cancellation={cancellation:?} error={error:?}"
        ))
        .into());
    }
    let close = nightly_support::close_producer_result(&producer, "cancellation producer close")?;
    match (error.delivery_status(), close) {
        (Some(DeliveryStatus::NotSent), Ok(())) => {}
        (Some(DeliveryStatus::PossiblySent), Err(ref close_error))
            if close_error.kind() == ErrorKind::State => {}
        (delivery, close) => {
            return Err(io::Error::other(format!(
                "producer close disagreed with cancellation terminal: delivery={delivery:?} close={close:?}"
            ))
            .into());
        }
    }
    fixture.finish()
}

fn cancel_within(
    delivery: &mut Delivery,
    operation_deadline: Instant,
) -> Result<CancellationOutcome, TestError> {
    loop {
        match delivery.cancel() {
            Ok(outcome) => return Ok(outcome),
            Err(error) if error.kind() == ErrorKind::Backpressure => {
                if Instant::now() >= operation_deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "producer cancellation remained contended until its operation deadline",
                    )
                    .into());
                }
                thread::yield_now();
            }
            Err(error) => return Err(error.into()),
        }
    }
}
