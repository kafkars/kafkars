//! Integrated host execution of one consumer-group offset alteration.

use std::{thread, time::Duration};

use crate::{
    AlterConsumerGroupOffsetTarget, AlterConsumerGroupOffsetsAdmissionErrorKind,
    AlterConsumerGroupOffsetsFailureKind, AlterConsumerGroupOffsetsOutcome,
    AlterConsumerGroupOffsetsRequest, Engine, EngineConfig,
};

#[test]
fn admitted_offset_alteration_reaches_one_host_terminal() {
    let timeout = Duration::from_millis(30);
    let engine = Engine::start(
        EngineConfig::new(vec!["192.0.2.1:9092".to_owned()]).with_admin_timeout(timeout),
    )
    .unwrap_or_else(|error| panic!("engine should start: {error}"));
    let admin = engine.admin();
    let accepted = loop {
        match admin.try_alter_consumer_group_offsets(request(), timeout) {
            Ok(accepted) => break accepted,
            Err(error)
                if error.kind() == AlterConsumerGroupOffsetsAdmissionErrorKind::Contended =>
            {
                thread::yield_now();
            }
            Err(error) => panic!("valid alteration should reach host admission: {error}"),
        }
    };

    let terminal = accepted
        .into_observer()
        .wait()
        .unwrap_or_else(|error| panic!("accepted alteration should remain observable: {error}"));
    let AlterConsumerGroupOffsetsOutcome::Failed(failure) = terminal else {
        panic!("test-net endpoint cannot alter committed offsets");
    };
    assert!(matches!(
        failure.kind(),
        AlterConsumerGroupOffsetsFailureKind::DeadlineElapsed
            | AlterConsumerGroupOffsetsFailureKind::DriverRejected
            | AlterConsumerGroupOffsetsFailureKind::Transport
    ));
    assert!(engine.shutdown().is_ok());
}

#[test]
fn local_deadline_rejection_returns_the_exact_request() {
    let engine = Engine::start(EngineConfig::new(vec!["192.0.2.1:9092".to_owned()]))
        .unwrap_or_else(|error| panic!("engine should start: {error}"));
    let request = request();
    let rejection = engine
        .admin()
        .try_alter_consumer_group_offsets(request.clone(), Duration::ZERO)
        .err()
        .unwrap_or_else(|| panic!("zero timeout must reject locally"));

    assert_eq!(
        rejection.kind(),
        AlterConsumerGroupOffsetsAdmissionErrorKind::InvalidDeadline
    );
    assert_eq!(rejection.into_request(), request);
    assert!(engine.shutdown().is_ok());
}

fn request() -> AlterConsumerGroupOffsetsRequest {
    AlterConsumerGroupOffsetsRequest::new(
        "payments".to_owned(),
        vec![AlterConsumerGroupOffsetTarget::new(
            "orders".to_owned(),
            0,
            91,
            Some(7),
            Some("checkpoint-a".to_owned()),
        )],
    )
}
