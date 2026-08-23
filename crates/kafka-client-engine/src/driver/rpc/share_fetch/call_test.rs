//! Accepted-call completion and shutdown recovery ownership evidence.

use std::time::{Duration, Instant};

use kafka_client_core::{Deadline, Moment, ShareFetchBrokerId};

use crate::{EngineConfig, clock::OperationDeadline, driver::DriverOwner};

use super::ShareFetchCall;
use super::terminal_test::prepared;

#[test]
fn completion_failure_returns_exact_response_correlation() {
    let driver = DriverOwner::build(&EngineConfig::new(vec!["127.0.0.1:1".to_owned()]))
        .unwrap_or_else(|error| panic!("driver owner: {error}"));
    let broker = ShareFetchBrokerId::try_from_raw(1).unwrap_or_else(|| panic!("valid broker"));
    let mut call = ShareFetchCall::submit(
        &driver,
        broker,
        prepared(),
        Moment::from_tick(10),
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(30),
            Instant::now() + Duration::from_secs(1),
        ),
    )
    .unwrap_or_else(|_failure| panic!("accepted ShareFetch call"));
    drop(driver);

    let failure = call
        .try_terminal()
        .unwrap_or_else(|| panic!("completion must be terminal"))
        .err()
        .unwrap_or_else(|| panic!("driver shutdown must fail completion"));
    let (evidence, kind) = failure.into_parts();
    assert_eq!(kind, super::ShareFetchCompletionErrorKind::Closed);
    let super::ShareFetchCallEvidence { correlation, .. } = evidence;
    assert!(correlation.contains(topic_id(), 0));
}

fn topic_id() -> [u8; 16] {
    let mut id = [0; 16];
    id[0] = 1;
    id
}
