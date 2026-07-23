//! Scenarios for accepted and immediately-ready private producer flush observation.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use kafka_client_engine::{Engine, EngineConfig};

use super::{producer::ProducerEngine, producer_flush::ProducerFlush};
use crate::{ErrorKind, KafkaError};

#[test]
fn accepted_empty_flush_uses_one_runtime_neutral_observer() {
    let engine = start_engine();
    let producer = ProducerEngine::new(engine.producer(), Duration::from_millis(50));
    let mut flush = producer.flush();
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let started = Instant::now();

    loop {
        match Pin::new(&mut flush).poll(&mut context) {
            Poll::Ready(result) => {
                assert_eq!(result, Ok(()));
                break;
            }
            Poll::Pending => {
                assert!(started.elapsed() < Duration::from_secs(1));
                std::hint::spin_loop();
            }
        }
    }

    let Poll::Ready(Err(error)) = Pin::new(&mut flush).poll(&mut context) else {
        panic!("an observed accepted flush must remain terminal")
    };
    assert_eq!(error.kind(), ErrorKind::State);
}

#[test]
fn ready_admission_error_is_not_retried_or_retimed() {
    let mut flush = ProducerFlush::ready(Err(KafkaError::new(ErrorKind::Backpressure, "capacity")));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    let Poll::Ready(Err(first)) = Pin::new(&mut flush).poll(&mut context) else {
        panic!("an admission error must already be ready")
    };
    assert_eq!(first.kind(), ErrorKind::Backpressure);

    let Poll::Ready(Err(second)) = Pin::new(&mut flush).poll(&mut context) else {
        panic!("a consumed ready result must remain terminal")
    };
    assert_eq!(second.kind(), ErrorKind::State);
}

fn start_engine() -> Engine {
    let result = Engine::start(EngineConfig::new(vec!["127.0.0.1:1".to_owned()]));
    let Ok(engine) = result else {
        panic!("valid local engine configuration should start")
    };
    engine
}
