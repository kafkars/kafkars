//! Exact broker and transport facts that authorize share-session replacement.

use std::sync::Arc;

use core::num::NonZeroI16;

use kafka_client_core::{
    Deadline, Moment, ShareFetchAttempt, partitioning::TopicMetadataGeneration,
};

use crate::{
    clock::DeadlineCapture,
    driver::{ShareFetchFailureKind, ShareFetchRoute, ShareFetchRouteRefresh},
    protocol::consumer::share_fetch::ShareFetchSuccess,
};

use super::super::fetch_session_set::ShareFetchSessionRecovery;

const SHARE_SESSION_NOT_FOUND: i16 = 122;
const INVALID_SHARE_SESSION_EPOCH: i16 = 123;

pub(super) fn driver_recovery(
    route: ShareFetchRoute,
    attempt: ShareFetchAttempt,
    submitted_at: Moment,
    now: Moment,
    kind: ShareFetchFailureKind,
) -> Result<ShareFetchSessionRecovery, ShareFetchRoute> {
    if !driver_recovery_authorized(kind) {
        return Err(route);
    }
    let Some(deadline) = replacement_deadline(attempt.deadline(), submitted_at, now) else {
        return Err(route);
    };
    ShareFetchRouteRefresh::try_new(route, deadline).map(ShareFetchSessionRecovery::route)
}

pub(super) fn route_recovery(
    route: ShareFetchRoute,
    attempt: ShareFetchAttempt,
    capture: DeadlineCapture,
    now: Moment,
    topic: Arc<str>,
    observed: TopicMetadataGeneration,
) -> Result<ShareFetchSessionRecovery, ShareFetchRoute> {
    if attempt.deadline() != capture.deadline() || capture.deadline().is_elapsed_at(now) {
        return Err(route);
    }
    ShareFetchRouteRefresh::try_new_with_metadata(
        route,
        capture.deadline(),
        capture.operation_deadline().transport(),
        topic,
        observed,
    )
    .map(ShareFetchSessionRecovery::route)
}

pub(super) const fn driver_recovery_authorized(kind: ShareFetchFailureKind) -> bool {
    matches!(
        kind,
        ShareFetchFailureKind::Transport | ShareFetchFailureKind::DeadlineElapsed
    )
}

pub(super) const fn replacement_deadline(
    original: Deadline,
    submitted_at: Moment,
    now: Moment,
) -> Option<Deadline> {
    let duration = original.tick().checked_sub(submitted_at.tick());
    match duration {
        Some(duration) if duration > 0 => now.checked_deadline_after(duration),
        _ => None,
    }
}

pub(super) const fn broker_recovery(code: NonZeroI16) -> Option<ShareFetchSessionRecovery> {
    match code.get() {
        SHARE_SESSION_NOT_FOUND | INVALID_SHARE_SESSION_EPOCH => {
            Some(ShareFetchSessionRecovery::session())
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ShareFetchResponseRecovery {
    None,
    Session,
    Route([u8; 16]),
    Terminal,
}

pub(super) fn response_recovery(success: &ShareFetchSuccess) -> ShareFetchResponseRecovery {
    let mut recovery = ShareFetchResponseRecovery::None;
    for topic in &success.topics {
        for code in topic.partitions.iter().flat_map(|partition| {
            partition
                .rejection
                .iter()
                .flat_map(|rejection| [rejection.fetch_error, rejection.acknowledge_error])
                .flatten()
        }) {
            match code.get() {
                SHARE_SESSION_NOT_FOUND | INVALID_SHARE_SESSION_EPOCH => {
                    if recovery == ShareFetchResponseRecovery::None {
                        recovery = ShareFetchResponseRecovery::Session;
                    }
                }
                3 | 6 | 56 | 74 | 100 => {
                    if !matches!(recovery, ShareFetchResponseRecovery::Route(_)) {
                        recovery = ShareFetchResponseRecovery::Route(topic.topic_id);
                    }
                }
                _ => return ShareFetchResponseRecovery::Terminal,
            }
        }
    }
    recovery
}
