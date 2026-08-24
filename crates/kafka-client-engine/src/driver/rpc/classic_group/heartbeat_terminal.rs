//! Raw classic Heartbeat terminal ownership retained for a protocol interpreter.

use kafka_client_core::{ClassicHeartbeatAttempt, GroupId, MembershipCycle};
use kafka_driver::{
    ApiVersion, CallFailure, CompletionError, ConnectionCloseReason, RequestError,
    ResponseCloseReason,
};
use kafka_wire::HeartbeatResponse;

use crate::clock::OperationDeadline;

use super::super::heartbeat_submission::ClassicHeartbeatSubmitError;

/// Exact correlation and deadline facts for one accepted Heartbeat call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClassicHeartbeatCallKey {
    group_id: GroupId,
    attempt: ClassicHeartbeatAttempt,
    deadline: OperationDeadline,
}

impl ClassicHeartbeatCallKey {
    pub(crate) const fn new(
        group_id: GroupId,
        attempt: ClassicHeartbeatAttempt,
        deadline: OperationDeadline,
    ) -> Self {
        Self {
            group_id,
            attempt,
            deadline,
        }
    }

    pub(crate) const fn group_id(self) -> GroupId {
        self.group_id
    }

    pub(crate) const fn cycle(self) -> MembershipCycle {
        self.attempt.cycle()
    }

    pub(crate) const fn attempt(self) -> ClassicHeartbeatAttempt {
        self.attempt
    }

    pub(crate) const fn deadline(self) -> OperationDeadline {
        self.deadline
    }

    pub(super) fn same_attempt_identity(self, other: Self) -> bool {
        self.group_id == other.group_id && self.attempt == other.attempt
    }
}

/// Uninterpreted generated response or driver-authoritative request failure.
#[must_use = "a raw Heartbeat terminal owns an unsettled membership attempt"]
pub(crate) struct ClassicHeartbeatTerminal {
    key: ClassicHeartbeatCallKey,
    selected_version: Option<i16>,
    result: Result<HeartbeatResponse, RequestError>,
}

impl ClassicHeartbeatTerminal {
    pub(crate) const fn key(&self) -> ClassicHeartbeatCallKey {
        self.key
    }

    pub(crate) const fn selected_version(&self) -> Option<i16> {
        self.selected_version
    }

    pub(crate) const fn result(&self) -> &Result<HeartbeatResponse, RequestError> {
        &self.result
    }

    pub(crate) fn coordinator_path_lost(&self) -> bool {
        self.result.as_ref().is_err_and(coordinator_path_lost)
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        ClassicHeartbeatCallKey,
        Option<i16>,
        Result<HeartbeatResponse, RequestError>,
    ) {
        (self.key, self.selected_version, self.result)
    }
}

pub(super) const fn coordinator_path_lost(error: &RequestError) -> bool {
    match error {
        RequestError::RouteUnavailable
        | RequestError::NameResolutionFailed { .. }
        | RequestError::ConnectionClosed(ResponseCloseReason::TransportClosed) => true,
        RequestError::Rejected { failure, .. } => matches!(
            failure,
            CallFailure::DeadlineExceeded
                | CallFailure::NotReady
                | CallFailure::Closed
                | CallFailure::ConnectionClosed {
                    reason: ConnectionCloseReason::OpenFailed(_)
                        | ConnectionCloseReason::TransportLost(_),
                }
        ),
        _ => false,
    }
}

pub(super) fn retain_classic_heartbeat_terminal(
    key: ClassicHeartbeatCallKey,
    selected_version: Option<ApiVersion>,
    result: Result<HeartbeatResponse, RequestError>,
) -> ClassicHeartbeatTerminal {
    ClassicHeartbeatTerminal {
        key,
        selected_version: selected_version.map(ApiVersion::value),
        result,
    }
}

/// Definitely-unsent driver rejection retaining exact attempt and deadline facts.
#[must_use = "a rejected Heartbeat call still owns its correlation facts"]
pub(crate) struct ClassicHeartbeatAdmissionFailure {
    key: ClassicHeartbeatCallKey,
    source: ClassicHeartbeatSubmitError,
}

impl ClassicHeartbeatAdmissionFailure {
    pub(super) const fn new(
        key: ClassicHeartbeatCallKey,
        source: ClassicHeartbeatSubmitError,
    ) -> Self {
        Self { key, source }
    }

    pub(crate) fn into_parts(self) -> (ClassicHeartbeatCallKey, ClassicHeartbeatSubmitError) {
        (self.key, self.source)
    }
}

/// Copyable observation while completion-corruption ownership stays retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ClassicHeartbeatCompletionObservation {
    key: ClassicHeartbeatCallKey,
    source: CompletionError,
}

impl ClassicHeartbeatCompletionObservation {
    pub(crate) const fn key(self) -> ClassicHeartbeatCallKey {
        self.key
    }

    pub(crate) const fn source(self) -> CompletionError {
        self.source
    }
}

/// Completion-cell failure retained until the driver owner is gone.
#[must_use = "completion failure retains one accepted Heartbeat attempt"]
pub(crate) struct ClassicHeartbeatCompletionFailure {
    key: ClassicHeartbeatCallKey,
    source: CompletionError,
}

impl ClassicHeartbeatCompletionFailure {
    pub(super) const fn new(key: ClassicHeartbeatCallKey, source: CompletionError) -> Self {
        Self { key, source }
    }

    pub(super) const fn observation(&self) -> ClassicHeartbeatCompletionObservation {
        ClassicHeartbeatCompletionObservation {
            key: self.key,
            source: self.source,
        }
    }

    pub(super) const fn key(&self) -> ClassicHeartbeatCallKey {
        self.key
    }

    pub(crate) fn into_parts(self) -> (ClassicHeartbeatCallKey, CompletionError) {
        (self.key, self.source)
    }
}

/// Accepted Heartbeat ownership recovered only after driver shutdown.
#[must_use = "recovered Heartbeat ownership still requires semantic settlement"]
pub(crate) struct RecoveredClassicHeartbeatCall {
    key: ClassicHeartbeatCallKey,
}

impl RecoveredClassicHeartbeatCall {
    pub(super) const fn new(key: ClassicHeartbeatCallKey) -> Self {
        Self { key }
    }

    pub(crate) const fn key(&self) -> ClassicHeartbeatCallKey {
        self.key
    }
}
