//! Forgotten-only Fetch request ownership and materialization scenarios.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafka_client_core::{Deadline, Moment};

use crate::{
    clock::OperationDeadline,
    protocol::fetch::{FetchRequestSettings, FetchSessionRequest, OwnedForgottenFetchPartition},
};

use super::forgotten::{ForgottenFetchRequest, ForgottenFetchSubmitFailureKind, materialize};

#[test]
fn materialization_preserves_session_settings_deadline_and_owned_identities() {
    let transport = Instant::now() + Duration::from_secs(2);
    let request = ForgottenFetchRequest::new(
        FetchRequestSettings::new(500, 1, 1_024, 1_024, 0),
        incremental(),
        OperationDeadline::from_parts_for_test(Deadline::from_tick(50_000_010), transport),
        vec![
            OwnedForgottenFetchPartition::new(Arc::from("alpha"), [1; 16], 1),
            OwnedForgottenFetchPartition::new(Arc::from("alpha"), [1; 16], 3),
        ],
    );

    let generated = materialize(&request, Moment::from_tick(10))
        .unwrap_or_else(|error| panic!("materialize forgotten Fetch: {error:?}"));

    assert_eq!((generated.session_id, generated.session_epoch), (91, 3));
    assert_eq!(generated.max_wait_ms, 50);
    assert!(generated.topics.is_empty());
    assert_eq!(generated.forgotten_topics_data.len(), 1);
    assert_eq!(generated.forgotten_topics_data[0].topic.as_str(), "alpha");
    assert_eq!(generated.forgotten_topics_data[0].partitions, vec![1, 3]);
    assert_eq!(request.deadline().transport(), transport);
}

#[test]
fn only_nonempty_live_incremental_deltas_are_admitted() {
    let empty = request(FetchSessionRequest::LEGACY, Vec::new());
    assert_eq!(
        materialize(&empty, Moment::from_tick(0)),
        Err(ForgottenFetchSubmitFailureKind::EmptyForgotten)
    );
    let legacy = request(
        FetchSessionRequest::LEGACY,
        vec![OwnedForgottenFetchPartition::new(
            Arc::from("alpha"),
            [1; 16],
            1,
        )],
    );
    assert_eq!(
        materialize(&legacy, Moment::from_tick(0)),
        Err(ForgottenFetchSubmitFailureKind::InvalidSession)
    );
}

#[test]
fn elapsed_request_returns_before_generated_request_construction() {
    let request = request(
        incremental(),
        vec![OwnedForgottenFetchPartition::new(
            Arc::from("alpha"),
            [1; 16],
            1,
        )],
    );
    assert_eq!(
        materialize(&request, Moment::from_tick(100)),
        Err(ForgottenFetchSubmitFailureKind::DeadlineElapsed)
    );
}

fn request(
    session: FetchSessionRequest,
    forgotten: Vec<OwnedForgottenFetchPartition>,
) -> ForgottenFetchRequest {
    ForgottenFetchRequest::new(
        FetchRequestSettings::new(500, 1, 1_024, 1_024, 0),
        session,
        OperationDeadline::from_parts_for_test(
            Deadline::from_tick(100),
            Instant::now() + Duration::from_secs(1),
        ),
        forgotten,
    )
}

fn incremental() -> FetchSessionRequest {
    FetchSessionRequest::incremental(91, 3).unwrap_or_else(|| panic!("incremental session"))
}
