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
    let Err(error) = clone.close().wait() else {
        panic!("only the first clone-shared close may succeed")
    };

    assert_eq!(error.kind(), ErrorKind::State);
    assert_eq!(error.delivery_status(), None);

    let Err(error) = producer.flush().wait() else {
        panic!("close must fence later flush barriers")
    };
    assert_eq!(error.kind(), ErrorKind::State);
    assert_eq!(error.delivery_status(), None);

    let Err(rejection) = producer.try_send(Record::to("orders").partition(0)) else {
        panic!("close must fence later record admission")
    };
    assert_eq!(rejection.error().kind(), ErrorKind::State);
    assert_eq!(
        rejection.error().delivery_status(),
        Some(DeliveryStatus::NotSent)
    );
}
