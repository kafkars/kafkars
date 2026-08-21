//! Public one-shot assigned-consumer construction scenarios.

use std::time::{Duration, Instant};

use crate::{AssignedConsumer, AssignedConsumerBuilder, Client, CloseAssignedConsumer, ErrorKind};

#[test]
fn builders_are_inert_until_one_build_claims_the_engine_owner() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start client: {error}"));
    let first_builder = client.assigned_consumer();
    let second_builder = first_builder.clone();

    let first = first_builder
        .build()
        .unwrap_or_else(|error| panic!("first assigned consumer: {error}"));
    let second = second_builder.build();

    let rejection = second.err().unwrap_or_else(|| {
        panic!("the clone-shared one-shot assigned consumer must reject a second build")
    });
    assert_eq!(rejection.error().kind(), ErrorKind::State);
    let (returned, error) = rejection.into_parts();
    assert_eq!(error.kind(), ErrorKind::State);
    let _: AssignedConsumerBuilder = returned;

    let mut first = first;
    close_when_admitted(&mut first)
        .wait()
        .unwrap_or_else(|error| panic!("close assigned consumer: {error}"));
}

#[test]
fn rejected_claim_returns_the_same_engine_builder_without_disturbing_the_winner() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("start client: {error}"));
    let winner = client.assigned_consumer();
    let rejected = winner.clone();
    let mut consumer = winner
        .build()
        .unwrap_or_else(|error| panic!("claim assigned consumer: {error}"));

    let rejection = rejected
        .build()
        .err()
        .unwrap_or_else(|| panic!("second claim must reject"));
    let (returned, error) = rejection.into_parts();
    assert_eq!(error.kind(), ErrorKind::State);

    let repeated = returned
        .build()
        .err()
        .unwrap_or_else(|| panic!("returned builder must still name the claimed engine"));
    assert_eq!(repeated.error().kind(), ErrorKind::State);

    close_when_admitted(&mut consumer)
        .wait()
        .unwrap_or_else(|error| panic!("close assigned consumer: {error}"));
}

fn close_when_admitted(consumer: &mut AssignedConsumer) -> CloseAssignedConsumer {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match consumer.try_close() {
            Ok(close) => return close,
            Err(error) if error.kind() == ErrorKind::Backpressure && Instant::now() < deadline => {
                std::hint::spin_loop();
            }
            Err(error) => panic!("admit assigned-consumer close: {error}"),
        }
    }
}
