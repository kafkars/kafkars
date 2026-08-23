//! Share-group acknowledgement lifecycle over one replicated Kafka partition.

use std::io;

use kafkars::{Producer, ShareConsumer, ShareDisposition};

use crate::real_broker_support::{TestError, unique_name};

use super::{evidence, nightly_support, share};

pub(crate) fn run_share_matrix() -> Result<(), TestError> {
    evidence::measure(
        "share_group_acknowledgement_lifecycle",
        acknowledgement_lifecycle,
    )
}

fn acknowledgement_lifecycle() -> Result<(), TestError> {
    let fixture = nightly_support::Fixture::new("share-ack", 1)?;
    let producer = fixture.client.producer().build()?;
    let group = unique_name("kafkars-share-ack");
    let mut first = share::build(&fixture.client, &group, &fixture.topic)?;
    share::await_assignment(&first, &fixture.topic)?;

    let first_offset = share::produce(&producer, &fixture.topic, b"member-one")?;
    let observed = share::receive_exact(&mut first, &fixture.topic, first_offset, b"member-one")?;
    share::accept_exact(&mut first, observed)?;

    let mut second = share::build(&fixture.client, &group, &fixture.topic)?;
    share::await_assignment(&second, &fixture.topic)?;
    share::close(first, "first share member close")?;
    let second_offset = share::produce(&producer, &fixture.topic, b"member-two")?;
    let observed = share::receive_exact(&mut second, &fixture.topic, second_offset, b"member-two")?;
    share::accept_exact(&mut second, observed)?;

    prove_accept(&producer, &fixture.topic, &mut second)?;
    prove_release(&producer, &fixture.topic, &mut second)?;
    prove_reject(&producer, &fixture.topic, &mut second)?;
    prove_lock_expiry(&producer, &fixture.topic, &mut second)?;

    share::close(second, "second share member close")?;
    nightly_support::close_producer(&producer, "share producer close")?;
    fixture.finish()
}

fn prove_accept(
    producer: &Producer,
    topic: &str,
    consumer: &mut ShareConsumer,
) -> Result<(), TestError> {
    let offset = share::produce(producer, topic, b"accept")?;
    let observed = share::receive_exact(consumer, topic, offset, b"accept")?;
    share::accept_exact(consumer, observed)?;
    let probe = share::produce(producer, topic, b"accept-probe")?;
    let observed = share::receive_exact(consumer, topic, probe, b"accept-probe")?;
    share::accept_exact(consumer, observed)
}

fn prove_release(
    producer: &Producer,
    topic: &str,
    consumer: &mut ShareConsumer,
) -> Result<(), TestError> {
    let offset = share::produce(producer, topic, b"release")?;
    let (batch, first_count) = share::receive_exact(consumer, topic, offset, b"release")?;
    share::acknowledge(consumer, batch, ShareDisposition::Release, "share release")?;
    let (batch, next_count) = share::receive_exact(consumer, topic, offset, b"release")?;
    require_higher_delivery_count(first_count, next_count, "released record")?;
    share::accept_exact(consumer, (batch, next_count))
}

fn prove_reject(
    producer: &Producer,
    topic: &str,
    consumer: &mut ShareConsumer,
) -> Result<(), TestError> {
    let offset = share::produce(producer, topic, b"reject")?;
    let (batch, _) = share::receive_exact(consumer, topic, offset, b"reject")?;
    share::acknowledge(consumer, batch, ShareDisposition::Reject, "share reject")?;
    let probe = share::produce(producer, topic, b"reject-probe")?;
    let observed = share::receive_exact(consumer, topic, probe, b"reject-probe")?;
    share::accept_exact(consumer, observed)
}

fn prove_lock_expiry(
    producer: &Producer,
    topic: &str,
    consumer: &mut ShareConsumer,
) -> Result<(), TestError> {
    let offset = share::produce(producer, topic, b"lock-expiry")?;
    let (batch, first_count) = share::receive_exact(consumer, topic, offset, b"lock-expiry")?;
    drop(batch);
    let (batch, next_count) = share::receive_exact(consumer, topic, offset, b"lock-expiry")?;
    require_higher_delivery_count(first_count, next_count, "unacknowledged record")?;
    share::accept_exact(consumer, (batch, next_count))
}

fn require_higher_delivery_count(first: i16, next: i16, label: &str) -> Result<(), TestError> {
    if next > first {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "{label} did not return with a higher delivery count: first={first} next={next}"
    ))
    .into())
}
