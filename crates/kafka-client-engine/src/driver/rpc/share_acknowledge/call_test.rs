//! Accepted-call completion and shutdown recovery ownership evidence.

use std::time::{Duration, Instant};

use kafka_client_core::{Deadline, ShareFetchBrokerId};

use crate::{
    EngineConfig, clock::OperationDeadline, driver::DriverOwner,
    protocol::consumer::share_acknowledge::test_support::prepared_request,
};

use super::ShareAcknowledgeCall;

#[test]
fn accepted_call_reports_completion_failure_after_driver_shutdown() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let broker = ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("valid broker"));
    let mut call = ShareAcknowledgeCall::submit(
        &driver,
        broker,
        prepared_request(),
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(30),
            Instant::now() + Duration::from_secs(1),
        ),
    )
    .unwrap_or_else(|_failure| panic!("accepted ShareAcknowledge call"));
    drop(driver);
    let failure = call
        .try_terminal()
        .unwrap_or_else(|| panic!("completion must be terminal"))
        .err()
        .unwrap_or_else(|| panic!("driver shutdown must fail completion"));
    let kind = failure.into_kind();
    assert_eq!(kind, super::ShareAcknowledgeCompletionErrorKind::Closed);
}
