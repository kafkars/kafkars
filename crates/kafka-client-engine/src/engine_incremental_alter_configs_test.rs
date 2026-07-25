//! Integrated host execution of one topic `IncrementalAlterConfigs` operation.

use std::{thread, time::Duration};

use crate::{
    Engine, EngineConfig, IncrementalAlterConfigsAdmissionErrorKind,
    IncrementalAlterConfigsFailureKind, IncrementalAlterConfigsOutcome,
    IncrementalAlterConfigsRequest, IncrementalConfigAlteration, IncrementalConfigOperation,
    TopicConfigAlterations,
};

#[test]
fn admitted_incremental_alter_configs_reaches_one_host_terminal() {
    let timeout = Duration::from_millis(30);
    let engine = Engine::start(
        EngineConfig::new(vec!["192.0.2.1:9092".to_owned()]).with_admin_timeout(timeout),
    )
    .unwrap_or_else(|error| panic!("engine should start: {error}"));
    let admin = engine.admin();
    let accepted = loop {
        match admin.try_incremental_alter_configs(request(), timeout) {
            Ok(accepted) => break accepted,
            Err(error) if error.kind() == IncrementalAlterConfigsAdmissionErrorKind::Contended => {
                thread::yield_now();
            }
            Err(error) => panic!("valid request should reach host admission: {error}"),
        }
    };

    let terminal = accepted
        .into_observer()
        .wait()
        .unwrap_or_else(|error| panic!("accepted operation should remain observable: {error}"));
    let IncrementalAlterConfigsOutcome::Failed(failure) = terminal else {
        panic!("test-net endpoint cannot return a broker result");
    };
    assert!(matches!(
        failure.kind(),
        IncrementalAlterConfigsFailureKind::DeadlineElapsed
            | IncrementalAlterConfigsFailureKind::DriverRejected
            | IncrementalAlterConfigsFailureKind::Transport
    ));
    assert!(engine.shutdown().is_ok());
}

fn request() -> IncrementalAlterConfigsRequest {
    IncrementalAlterConfigsRequest::new(vec![TopicConfigAlterations::new(
        "orders".to_owned(),
        vec![IncrementalConfigAlteration::new(
            "retention.ms".to_owned(),
            IncrementalConfigOperation::Delete,
        )],
    )])
}
