//! Direct bounded shutdown-state admission and terminal retention scenario.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use kafka_client_engine::{Engine, EngineConfig};

use super::state::ShutdownShared;

struct WakeProbe {
    called: AtomicBool,
}

impl Wake for WakeProbe {
    fn wake(self: Arc<Self>) {
        self.called.store(true, Ordering::Release);
    }
}

#[test]
fn one_state_owner_retains_the_terminal_after_its_worker_finishes() {
    let engine = Engine::start(EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("start engine: {error}"));
    let shared =
        ShutdownShared::try_new(engine).unwrap_or_else(|error| panic!("reserve state: {error}"));
    let wake = Arc::new(WakeProbe {
        called: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&wake));
    let context = Context::from_waker(&waker);
    let mut registration = None;

    assert!(shared.poll(&mut registration, &context).is_pending());

    shared.begin();

    assert!(shared.wait().is_ok());
    shared.join_worker();
    assert!(wake.called.load(Ordering::Acquire));
    assert!(matches!(
        shared.poll(&mut registration, &context),
        Poll::Ready(Ok(()))
    ));
    assert!(shared.wait().is_ok());
}
