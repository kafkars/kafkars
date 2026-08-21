//! Public producer close type and lifecycle scenarios.

use std::{
    future::Future,
    time::{Duration, Instant},
};

use crate::{Client, DeliveryStatus, ErrorKind, KafkaError, Record};

use super::CloseProducer;

#[test]
fn named_close_exposes_async_and_blocking_observation_on_one_type() {
    fn assert_future<T: Future<Output = Result<(), KafkaError>>>() {}
    fn assert_send<T: Send>() {}
    fn assert_wait(_: fn(CloseProducer) -> Result<(), KafkaError>) {}

    assert_future::<CloseProducer>();
    assert_send::<CloseProducer>();
    assert_wait(CloseProducer::wait);
}

#[test]
fn public_close_is_clone_shared_and_first_success_wins() {
    let result = Client::builder().bootstrap_servers(["127.0.0.1:1"]).build();
    let Ok(client) = result else {
        panic!("valid local client configuration should build")
    };
    let result = client.producer().build();
    let Ok(producer) = result else {
        panic!("producer construction should remain local")
    };
    let clone = producer.clone();

    let admission_deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match producer.close().wait() {
            Ok(()) => break,
            Err(error) if error.kind() == ErrorKind::Backpressure => {
                assert!(
                    Instant::now() < admission_deadline,
                    "one close should be admitted after startup contention"
                );
                std::hint::spin_loop();
            }
            Err(error) => panic!("first close should succeed: {error}"),
        }
    }
    let error = state_after_contention(admission_deadline, || clone.close().wait());

    assert_eq!(error.kind(), ErrorKind::State);
    assert_eq!(error.delivery_status(), None);

    let error = state_after_contention(admission_deadline, || producer.flush().wait());
    assert_eq!(error.kind(), ErrorKind::State);
    assert_eq!(error.delivery_status(), None);

    let mut record = Record::to("orders").partition(0);
    let rejection = loop {
        match producer.try_send(record) {
            Err(rejection) if rejection.error().kind() == ErrorKind::Backpressure => {
                assert!(
                    Instant::now() < admission_deadline,
                    "closed state should become observable after record-admission contention"
                );
                record = rejection.into_parts().0;
                std::hint::spin_loop();
            }
            Err(rejection) => break rejection,
            Ok(_) => panic!("close must fence later record admission"),
        }
    };
    assert_eq!(rejection.error().kind(), ErrorKind::State);
    assert_eq!(
        rejection.error().delivery_status(),
        Some(DeliveryStatus::NotSent)
    );
}

fn state_after_contention(
    deadline: Instant,
    mut attempt: impl FnMut() -> Result<(), KafkaError>,
) -> KafkaError {
    loop {
        match attempt() {
            Err(error) if error.kind() == ErrorKind::Backpressure => {
                assert!(
                    Instant::now() < deadline,
                    "closed state should become observable after barrier contention"
                );
                std::hint::spin_loop();
            }
            Err(error) => return error,
            Ok(()) => panic!("only the first clone-shared close may succeed"),
        }
    }
}
