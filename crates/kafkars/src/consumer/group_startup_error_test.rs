//! Public retained startup-error observation after accepted modern membership ownership.

use std::{
    thread,
    time::{Duration, Instant},
};

use super::ConsumerGroupProtocol;
use crate::{Client, ErrorKind, RetryAdvice};

#[test]
fn accepted_modern_start_exposes_its_later_terminal_instead_of_staying_pending() {
    let client = Client::builder()
        .bootstrap_servers(["127.0.0.1:1"])
        .build()
        .unwrap_or_else(|error| panic!("lazy client: {error}"));
    let mut builder = client
        .consumer("startup-terminal")
        .subscribe(["orders"])
        .group_protocol(ConsumerGroupProtocol::Consumer)
        .membership_start_timeout(Duration::from_millis(50));
    let admission_deadline = Instant::now() + Duration::from_secs(2);
    let consumer = loop {
        match builder.build() {
            Ok(consumer) => break consumer,
            Err(rejected)
                if rejected.error().retry_advice() == RetryAdvice::RetrySafe
                    && Instant::now() < admission_deadline =>
            {
                (builder, _) = rejected.into_parts();
                std::hint::spin_loop();
            }
            Err(error) => panic!("accepted consumer start: {error}"),
        }
    };
    let observation_deadline = Instant::now() + Duration::from_secs(3);
    let error = loop {
        if let Some(error) = consumer.startup_error() {
            break error;
        }
        match consumer.assignment() {
            Ok(None) => {}
            Err(error) if error.kind() == ErrorKind::Backpressure => {}
            Ok(Some(assignment)) => panic!("startup failure exposed assignment: {assignment:?}"),
            Err(error) => panic!("startup observation failed unexpectedly: {error}"),
        }
        assert!(
            Instant::now() < observation_deadline,
            "accepted startup terminal remained silent"
        );
        thread::sleep(Duration::from_millis(5));
    };
    assert!(error.is_fatal());
}
