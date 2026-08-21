//! Scenarios for shared accepted and immediately-ready producer barriers.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use kafka_client_engine::{Engine, EngineConfig};

use super::{
    barrier::{BarrierKind, ProducerBarrier},
    handle::ProducerEngine,
};
use crate::{ErrorKind, KafkaError};

#[test]
fn accepted_empty_flush_uses_one_runtime_neutral_observer() {
    let engine = start_engine();
    let producer = ProducerEngine::new(engine.producer(), Duration::from_millis(50));
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let admission_deadline = Instant::now() + Duration::from_secs(1);

    'admission: loop {
        let mut flush = producer.flush();
        let first = Pin::new(&mut flush).poll(&mut context);
        if let Poll::Ready(Err(error)) = &first
            && error.kind() == ErrorKind::Backpressure
        {
            assert!(
                Instant::now() < admission_deadline,
                "one empty flush should be admitted after startup contention"
            );
            std::hint::spin_loop();
            continue;
        }

        let result = match first {
            Poll::Ready(result) => result,
            Poll::Pending => loop {
                match Pin::new(&mut flush).poll(&mut context) {
                    Poll::Ready(result) => break result,
                    Poll::Pending => {
                        assert!(
                            Instant::now() < admission_deadline,
                            "one accepted empty flush should settle"
                        );
                        std::hint::spin_loop();
                    }
                }
            },
        };
        assert_eq!(result, Ok(()));

        let Poll::Ready(Err(error)) = Pin::new(&mut flush).poll(&mut context) else {
            panic!("an observed accepted flush must remain terminal")
        };
        assert_eq!(error.kind(), ErrorKind::State);
        break 'admission;
    }
}

#[test]
fn ready_admission_error_is_not_retried_or_retimed() {
    let mut flush = ProducerBarrier::ready(
        BarrierKind::Flush,
        Err(KafkaError::new(ErrorKind::Backpressure, "capacity")),
    );
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

#[test]
fn consumed_close_result_remains_close_specific() {
    let mut close = ProducerBarrier::ready(
        BarrierKind::Close,
        Err(KafkaError::new(ErrorKind::State, "already closed")),
    );
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    let Poll::Ready(Err(first)) = Pin::new(&mut close).poll(&mut context) else {
        panic!("a close rejection must already be ready")
    };
    assert_eq!(first.delivery_status(), None);

    let Poll::Ready(Err(second)) = Pin::new(&mut close).poll(&mut context) else {
        panic!("a consumed close result must remain terminal")
    };
    assert_eq!(second.kind(), ErrorKind::State);
    assert_eq!(second.to_string(), "producer close was already observed");
}

fn start_engine() -> Engine {
    let result = Engine::start(EngineConfig::new(vec!["127.0.0.1:1".to_owned()]));
    let Ok(engine) = result else {
        panic!("valid local engine configuration should start")
    };
    engine
}
