//! Integrated host execution of one consumer-group offset deletion.

use std::{thread, time::Duration};

use crate::{
    DeleteConsumerGroupOffsetTarget, DeleteConsumerGroupOffsetsAdmissionErrorKind,
    DeleteConsumerGroupOffsetsFailureKind, DeleteConsumerGroupOffsetsOutcome,
    DeleteConsumerGroupOffsetsRequest, Engine, EngineConfig,
};

#[test]
fn admitted_offset_deletion_reaches_one_host_terminal() {
    let timeout = Duration::from_millis(30);
    let engine = Engine::start(
        EngineConfig::new(vec!["192.0.2.1:9092".to_owned()]).with_admin_timeout(timeout),
    )
    .unwrap_or_else(|error| panic!("engine should start: {error}"));
    let admin = engine.admin();
    let accepted = loop {
        match admin.try_delete_consumer_group_offsets(request(), timeout) {
            Ok(accepted) => break accepted,
            Err(error)
                if error.kind() == DeleteConsumerGroupOffsetsAdmissionErrorKind::Contended =>
            {
                thread::yield_now();
            }
            Err(error) => panic!("valid deletion should reach host admission: {error}"),
        }
    };

    let terminal = accepted
        .into_observer()
        .wait()
        .unwrap_or_else(|error| panic!("accepted deletion should remain observable: {error}"));
    let DeleteConsumerGroupOffsetsOutcome::Failed(failure) = terminal else {
        panic!("test-net endpoint cannot delete committed offsets");
    };
    assert!(matches!(
        failure.kind(),
        DeleteConsumerGroupOffsetsFailureKind::DeadlineElapsed
            | DeleteConsumerGroupOffsetsFailureKind::DriverRejected
            | DeleteConsumerGroupOffsetsFailureKind::Transport
    ));
    assert!(engine.shutdown().is_ok());
}

fn request() -> DeleteConsumerGroupOffsetsRequest {
    DeleteConsumerGroupOffsetsRequest::new(
        "payments".to_owned(),
        vec![DeleteConsumerGroupOffsetTarget::new("orders".to_owned(), 0)],
    )
}
