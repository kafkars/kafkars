//! Scenarios for one observer shared by async polling and blocking wait.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use kafka_client_engine::{Engine, EngineConfig};

use super::producer::ProducerEngine;
use crate::{DeliveryStatus, ErrorKind, Record};

#[test]
fn accepted_delivery_is_one_runtime_neutral_observer() {
    let engine = start_engine();
    let mut delivery = accepted_delivery(&engine, Duration::from_millis(50));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    let terminal = match Pin::new(&mut delivery).poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => delivery.wait(),
    };
    let Err(error) = terminal else {
        panic!("an unreachable bootstrap endpoint cannot acknowledge delivery")
    };

    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(error.delivery_status(), Some(DeliveryStatus::NotSent));
}

#[test]
fn polling_after_terminal_observation_is_a_safe_state_error() {
    let engine = start_engine();
    let mut delivery = accepted_delivery(&engine, Duration::from_millis(10));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let started = Instant::now();

    let first = loop {
        match Pin::new(&mut delivery).poll(&mut context) {
            Poll::Ready(result) => break result,
            Poll::Pending => {
                assert!(started.elapsed() < Duration::from_secs(1));
                std::hint::spin_loop();
            }
        }
    };
    let Err(first_error) = first else {
        panic!("an unreachable bootstrap endpoint cannot acknowledge delivery")
    };
    assert_eq!(first_error.kind(), ErrorKind::Timeout);

    let Poll::Ready(Err(second_error)) = Pin::new(&mut delivery).poll(&mut context) else {
        panic!("a consumed observer must return a ready state error")
    };
    assert_eq!(second_error.kind(), ErrorKind::State);
    assert_eq!(second_error.delivery_status(), None);
}

fn accepted_delivery(
    engine: &Engine,
    timeout: Duration,
) -> super::producer_delivery::ProducerDelivery {
    let producer = ProducerEngine::new(engine.producer(), timeout);
    let mut record = Record::to("orders")
        .partition(0)
        .key("order-42")
        .value("created");
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match producer.try_send(record) {
            Ok(delivery) => return delivery,
            Err(rejection) => {
                let (returned, error) = rejection.into_parts();
                assert_eq!(error.kind(), ErrorKind::Backpressure);
                assert!(
                    Instant::now() < deadline,
                    "one valid explicit-partition record should be admitted"
                );
                record = returned;
                std::hint::spin_loop();
            }
        }
    }
}

fn start_engine() -> Engine {
    let result = Engine::start(EngineConfig::new(vec!["127.0.0.1:1".to_owned()]));
    let Ok(engine) = result else {
        panic!("valid local engine configuration should start")
    };
    engine
}
