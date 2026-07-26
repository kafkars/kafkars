//! Integrated host execution of one consumer-group offset query.

use std::{thread, time::Duration};

use crate::{
    Engine, EngineConfig, ListConsumerGroupOffsetsAdmissionErrorKind,
    ListConsumerGroupOffsetsFailureKind, ListConsumerGroupOffsetsOutcome,
    ListConsumerGroupOffsetsRequest,
};

#[test]
fn admitted_group_offset_query_reaches_one_host_terminal() {
    let timeout = Duration::from_millis(30);
    let engine = Engine::start(
        EngineConfig::new(vec!["192.0.2.1:9092".to_owned()]).with_admin_timeout(timeout),
    )
    .unwrap_or_else(|error| panic!("engine should start: {error}"));
    let admin = engine.admin();
    let accepted = loop {
        match admin.try_list_consumer_group_offsets(request(), timeout) {
            Ok(accepted) => break accepted,
            Err(error) if error.kind() == ListConsumerGroupOffsetsAdmissionErrorKind::Contended => {
                thread::yield_now();
            }
            Err(error) => panic!("valid request should reach host admission: {error}"),
        }
    };

    let terminal = accepted
        .into_observer()
        .wait()
        .unwrap_or_else(|error| panic!("accepted operation should remain observable: {error}"));
    let ListConsumerGroupOffsetsOutcome::Failed(failure) = terminal else {
        panic!("test-net endpoint cannot return group offsets");
    };
    assert!(matches!(
        failure.kind(),
        ListConsumerGroupOffsetsFailureKind::DeadlineElapsed
            | ListConsumerGroupOffsetsFailureKind::DriverRejected
            | ListConsumerGroupOffsetsFailureKind::Transport
    ));
    assert!(engine.shutdown().is_ok());
}

fn request() -> ListConsumerGroupOffsetsRequest {
    ListConsumerGroupOffsetsRequest::new("payments".to_owned(), true)
}
