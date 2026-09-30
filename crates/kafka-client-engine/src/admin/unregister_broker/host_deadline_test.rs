//! A pending controller invalidation cannot extend broker unregistration's deadline.

use kafka_client_core::{Moment, UnregisterBrokerPlan};

use crate::{
    EngineConfig,
    admin::AdminCompletionNotifier,
    clock::MonotonicClock,
    driver::{DriverOwner, UnregisterBrokerCall},
};

use super::{
    UnregisterBrokerDeliveryStatus, UnregisterBrokerFailureKind, UnregisterBrokerHost,
    UnregisterBrokerOutcome, UnregisterBrokerTurn,
};

#[test]
fn pending_controller_refresh_expires_without_admitting_a_retry() {
    let (mut notifier, ports) =
        AdminCompletionNotifier::start().unwrap_or_else(|error| panic!("notifier: {error}"));
    let mut host = UnregisterBrokerHost::new(ports.unregister_broker);
    let capture = MonotonicClock::new()
        .capture_deadline_after(std::time::Duration::from_secs(5))
        .unwrap_or_else(|error| panic!("capture deadline: {error}"));
    let plan = UnregisterBrokerPlan::new(3).unwrap_or_else(|error| panic!("valid broker: {error}"));
    let admission = host
        .try_admit(capture.now(), capture.operation_deadline(), plan)
        .unwrap_or_else(|error| panic!("admit unregistration: {error:?}"));
    let UnregisterBrokerTurn::Submit(submission) = host
        .turn(capture.now(), None)
        .unwrap_or_else(|error| panic!("submission turn: {error}"))
    else {
        panic!("submission required");
    };
    let (id, deadline, plan, _) = submission.into_parts();
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver: {error}"));
    let call = UnregisterBrokerCall::submit(&driver, plan, deadline.transport(), false)
        .unwrap_or_else(|_| panic!("accepted call"));
    host.accept_call(id, call)
        .unwrap_or_else(|error| panic!("accept call: {error}"));
    host.replace_call_with_controller_route_failure_for_test(plan);
    assert_eq!(host.next_deadline(), Some(deadline.core()));
    assert!(matches!(
        host.turn(Moment::from_tick(deadline.core().tick()), Some(&driver)),
        Ok(UnregisterBrokerTurn::Progress)
    ));
    assert_eq!(host.unsettled(), 0);
    let UnregisterBrokerOutcome::Failed(failure) = admission
        .observer
        .wait()
        .unwrap_or_else(|error| panic!("observe deadline: {error}"))
    else {
        panic!("deadline failure required");
    };
    assert_eq!(failure.kind(), UnregisterBrokerFailureKind::DeadlineElapsed);
    assert_eq!(failure.delivery(), UnregisterBrokerDeliveryStatus::NotSent);
    drop((driver, host));
    notifier
        .stop()
        .unwrap_or_else(|error| panic!("stop notifier: {error}"))
        .join_off_notifier()
        .unwrap_or_else(|_| panic!("join notifier"));
}
