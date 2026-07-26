//! Integrated fixed-domain notifier workers and reentrant-shutdown scenarios.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, mpsc::sync_channel},
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use crate::{Engine, EngineConfig, EngineShutdownError, EngineShutdownErrorKind};

#[test]
fn startup_owns_one_worker_for_each_completion_domain() {
    let engine = start(Duration::from_millis(80));
    assert_eq!(engine.completion_notifier_thread_count(), 5);
    assert!(engine.shutdown().is_ok());
}

#[test]
fn admin_notifier_reentrant_shutdown_defers_its_own_join() {
    let timeout = Duration::from_millis(200);
    let engine = start(timeout);
    let accepted = engine
        .admin()
        .try_describe_cluster(timeout)
        .unwrap_or_else(|error| panic!("DescribeCluster admission should succeed: {error}"));
    let mut observer = accepted.into_observer();
    let (result_sender, result_receiver) = sync_channel(1);
    let waker = Waker::from(Arc::new(AdminShutdownWake {
        engine: engine.clone(),
        result_sender,
    }));
    assert_eq!(
        Pin::new(&mut observer).poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    );

    let result = result_receiver
        .recv_timeout(Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("admin notifier shutdown should arrive: {error}"));
    assert_eq!(
        result.err().map(|error| error.kind()),
        Some(EngineShutdownErrorKind::NotifierThread)
    );
    assert!(engine.shutdown().is_ok());
    let _terminal = observer
        .wait()
        .unwrap_or_else(|error| panic!("accepted DescribeCluster must settle: {error}"));
}

fn start(admin_timeout: Duration) -> Engine {
    Engine::start(
        EngineConfig::new(vec!["192.0.2.1:9092".to_owned()]).with_admin_timeout(admin_timeout),
    )
    .unwrap_or_else(|error| panic!("engine should start: {error}"))
}

struct AdminShutdownWake {
    engine: Engine,
    result_sender: std::sync::mpsc::SyncSender<Result<(), EngineShutdownError>>,
}

impl Wake for AdminShutdownWake {
    fn wake(self: Arc<Self>) {
        let _sent = self.result_sender.send(self.engine.shutdown());
    }
}
