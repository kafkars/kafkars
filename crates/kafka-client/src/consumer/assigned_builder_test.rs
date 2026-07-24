//! Public one-shot assigned-consumer construction scenarios.

use crate::{Client, ErrorKind};

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
    first
        .try_close()
        .unwrap_or_else(|error| panic!("admit assigned-consumer close: {error}"))
        .wait()
        .unwrap_or_else(|error| panic!("close assigned consumer: {error}"));
}
