//! Bounded admission, deadline, shutdown, and retained-byte recovery.

use std::{io, time::Duration};

use kafkars::{DeliveryStatus, ErrorKind, ProducerLimits, Record};

use crate::real_broker_support::{
    OPERATION_TIMEOUT, TestError, client_builder_from_environment, wait_within,
};

use super::{nightly_control::BrokerGuard, nightly_support};

pub(super) fn bounded_admission_deadlines_and_shutdown() -> Result<(), TestError> {
    let limits = ProducerLimits::new(1_024, 1, 1, 1_024, 1, 1_024, Duration::from_millis(10));
    let builder = client_builder_from_environment("kafkars-nightly-bounds")?
        .producer_limits(limits)
        .producer_delivery_timeout(Duration::from_millis(500));
    let mut fixture = nightly_support::Fixture::from_builder(builder, "bounded", 1)?;
    let warmup_producer = fixture
        .client
        .producer()
        .delivery_timeout(OPERATION_TIMEOUT)
        .build()?;
    wait_within(
        warmup_producer.send(
            Record::to(fixture.topic.as_str())
                .partition(0)
                .value("warmup"),
        ),
        "bounded admission warmup",
    )??;
    drop(warmup_producer);
    let producer = fixture.client.producer().build()?;
    let leader = nightly_support::describe_topic(&fixture.client.admin(), &fixture.topic)?
        .partitions()[0]
        .leader_id()
        .ok_or_else(|| io::Error::other("bounded topic has no leader"))?;
    let paused = BrokerGuard::pause(leader)?;
    let first = producer.try_send(
        Record::to(fixture.topic.as_str())
            .partition(0)
            .value(vec![7_u8; 800]),
    )?;
    let rejection = match producer.try_send(
        Record::to(fixture.topic.as_str())
            .partition(0)
            .value(vec![8_u8; 800]),
    ) {
        Ok(_) => return Err(io::Error::other("the second record exceeded its active slot").into()),
        Err(rejection) => rejection,
    };
    if rejection.error().kind() != ErrorKind::Backpressure {
        return Err(io::Error::other("bounded admission did not report backpressure").into());
    }
    let timed_out = match wait_within(first, "bounded delivery deadline")? {
        Ok(_) => return Err(io::Error::other("paused broker acknowledged Produce").into()),
        Err(error) => error,
    };
    paused.restore()?;
    if timed_out.kind() != ErrorKind::Timeout
        || !matches!(
            timed_out.delivery_status(),
            Some(DeliveryStatus::NotSent | DeliveryStatus::PossiblySent)
        )
    {
        return Err(io::Error::other(format!(
            "deadline did not preserve authoritative delivery certainty: {timed_out:?}"
        ))
        .into());
    }

    let close = nightly_support::close_producer_result(&producer, "bounded producer close")?;
    match (timed_out.delivery_status(), close) {
        (Some(DeliveryStatus::NotSent), Ok(())) => {}
        (Some(DeliveryStatus::PossiblySent), Err(ref close_error))
            if close_error.kind() == ErrorKind::State => {}
        (delivery, close) => {
            return Err(io::Error::other(format!(
                "producer close disagreed with deadline terminal: delivery={delivery:?} close={close:?}"
            ))
            .into());
        }
    }
    fixture.remove_topic()?;
    wait_within(fixture.client.shutdown(), "bounded client shutdown")??;
    let closed = match producer.try_send(
        Record::to(fixture.topic.as_str())
            .partition(0)
            .value("after-shutdown"),
    ) {
        Ok(_) => return Err(io::Error::other("shutdown admitted a later record").into()),
        Err(rejection) => rejection,
    };
    if closed.error().kind() != ErrorKind::State {
        return Err(io::Error::other("shutdown did not report closed producer state").into());
    }
    Ok(())
}

pub(super) fn retained_byte_recovery() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("retained-bytes", 1)?;
    let producer = fixture.client.producer().build()?;
    let records = (0_u8..20)
        .map(|index| {
            let mut value = vec![index; 32 * 1024];
            value.extend_from_slice(index.to_string().as_bytes());
            Record::to(fixture.topic.as_str()).partition(0).value(value)
        })
        .collect();
    let result = wait_within(producer.send_batch(records), "retained-byte batch")?;
    if result.rejection().is_some() || result.deliveries().iter().any(Result::is_err) {
        return Err(io::Error::other("retained-byte batch did not fully settle").into());
    }
    drop(result);
    nightly_support::flush_producer(&producer, "retained-byte flush")?;
    let metrics = nightly_support::poll_until(|| {
        let snapshot = wait_within(fixture.client.metrics().ok()?, "retained-byte metrics")
            .ok()?
            .ok()?;
        let pressure = snapshot.producer();
        (pressure.active_records() == 0
            && pressure.active_bytes() == 0
            && pressure.waiting_records() == 0
            && pressure.waiting_bytes() == 0
            && pressure.prepared_batches() == 0
            && pressure.prepared_batch_bytes() == 0
            && pressure.terminal_backlog() == 0)
            .then_some(snapshot)
    })?;
    if metrics.mailbox().queued_work_bytes() != 0 || metrics.mailbox().queued_control_bytes() != 0 {
        return Err(
            io::Error::other("driver mailbox retained bytes after terminal recovery").into(),
        );
    }
    nightly_support::close_producer(&producer, "retained-byte producer close")?;
    fixture.finish()
}
