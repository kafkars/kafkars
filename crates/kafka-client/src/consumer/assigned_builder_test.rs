//! Public one-shot assigned-consumer construction scenarios.

use std::time::{Duration, Instant};

use crate::{AssignedConsumer, Client, CloseAssignedConsumer, ErrorKind};

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

    assert!(matches!(second, Err(error) if error.kind() == ErrorKind::State));
    let mut first = first;
    close_when_admitted(&mut first)
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
