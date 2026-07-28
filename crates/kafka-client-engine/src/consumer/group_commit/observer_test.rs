//! Named Future observation over the same accepted commit terminal cell.

use std::{
    future::Future,
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use super::{GroupConsumerCommitOutcome, test_support::GroupCommitFixture};

#[test]
fn future_poll_observes_the_same_recovered_terminal_as_blocking_wait() {
    let mut fixture = GroupCommitFixture::start(false);
    let checkpoint = fixture.take_checkpoint();
    let accepted = fixture
        .handle
        .try_commit(checkpoint, Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("commit admission: {error}"));
    let mut observer = Box::pin(accepted.into_observer());
    let (wake_tx, wake_rx) = mpsc::channel();
    let waker = Waker::from(Arc::new(TestWake(wake_tx)));
    let mut context = Context::from_waker(&waker);

    assert!(matches!(
        Future::poll(observer.as_mut(), &mut context),
        Poll::Pending
    ));
    let mut registry = fixture.owner.terminal_registry();
    registry
        .recover_after_driver_shutdown()
        .unwrap_or_else(|error| panic!("registry recovery: {error}"));
    wake_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|error| panic!("commit observer was not woken: {error}"));
    let Poll::Ready(Ok(GroupConsumerCommitOutcome::Failed(_failure, _checkpoint))) =
        Future::poll(observer.as_mut(), &mut context)
    else {
        panic!("recovered commit terminal must become ready");
    };

    drop(observer);
    let join = registry
        .finish_shutdown()
        .unwrap_or_else(|error| panic!("registry finish: {error}"));
    drop(registry);
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

struct TestWake(mpsc::Sender<()>);

impl Wake for TestWake {
    fn wake(self: Arc<Self>) {
        let _result = self.0.send(());
    }
}
