//! Raw `JoinGroup` terminal ownership retained for a later protocol interpreter.

use kafka_client_core::{GroupId, MembershipCycle};
use kafka_driver::{ApiVersion, CompletionError, RequestError};
use kafka_wire::JoinGroupResponse;

use crate::clock::OperationDeadline;

use super::super::join_group_submission::JoinGroupSubmitError;

/// Exact correlation and deadline facts for one accepted Join call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JoinGroupCallKey {
    group_id: GroupId,
    cycle: MembershipCycle,
    deadline: OperationDeadline,
}

impl JoinGroupCallKey {
    pub(crate) const fn new(
        group_id: GroupId,
        cycle: MembershipCycle,
        deadline: OperationDeadline,
    ) -> Self {
        Self {
            group_id,
            cycle,
            deadline,
        }
    }

    pub(crate) const fn group_id(self) -> GroupId {
        self.group_id
    }

    pub(crate) const fn cycle(self) -> MembershipCycle {
        self.cycle
    }

    pub(crate) const fn deadline(self) -> OperationDeadline {
        self.deadline
    }

    pub(super) fn same_cycle_identity(self, other: Self) -> bool {
        self.group_id == other.group_id && self.cycle == other.cycle
    }
}

/// Uninterpreted generated response or driver-authoritative request failure.
#[must_use = "a raw JoinGroup terminal owns an unsettled membership cycle"]
pub(crate) struct JoinGroupTerminal {
    key: JoinGroupCallKey,
    selected_version: Option<i16>,
    result: Result<JoinGroupResponse, RequestError>,
}

impl JoinGroupTerminal {
    pub(crate) const fn key(&self) -> JoinGroupCallKey {
        self.key
    }

    pub(crate) const fn selected_version(&self) -> Option<i16> {
        self.selected_version
    }

    pub(crate) const fn result(&self) -> &Result<JoinGroupResponse, RequestError> {
        &self.result
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        JoinGroupCallKey,
        Option<i16>,
        Result<JoinGroupResponse, RequestError>,
    ) {
        (self.key, self.selected_version, self.result)
    }
}

pub(super) fn retain_join_group_terminal(
    key: JoinGroupCallKey,
    selected_version: Option<ApiVersion>,
    result: Result<JoinGroupResponse, RequestError>,
) -> JoinGroupTerminal {
    JoinGroupTerminal {
        key,
        selected_version: selected_version.map(ApiVersion::value),
        result,
    }
}

/// Definitely-unsent driver rejection retaining exact cycle and deadline facts.
#[must_use = "a rejected JoinGroup call still owns its correlation facts"]
pub(crate) struct JoinGroupAdmissionFailure {
    key: JoinGroupCallKey,
    source: JoinGroupSubmitError,
}

impl JoinGroupAdmissionFailure {
    pub(super) const fn new(key: JoinGroupCallKey, source: JoinGroupSubmitError) -> Self {
        Self { key, source }
    }

    pub(crate) fn into_parts(self) -> (JoinGroupCallKey, JoinGroupSubmitError) {
        (self.key, self.source)
    }
}

/// Copyable observation while completion-corruption ownership stays retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JoinGroupCompletionObservation {
    key: JoinGroupCallKey,
    source: CompletionError,
}

impl JoinGroupCompletionObservation {
    pub(crate) const fn key(self) -> JoinGroupCallKey {
        self.key
    }

    pub(crate) const fn source(self) -> CompletionError {
        self.source
    }
}

/// Completion-cell failure retained until the driver owner is gone.
#[must_use = "completion failure retains one accepted JoinGroup cycle"]
pub(crate) struct JoinGroupCompletionFailure {
    key: JoinGroupCallKey,
    source: CompletionError,
}

impl JoinGroupCompletionFailure {
    pub(super) const fn new(key: JoinGroupCallKey, source: CompletionError) -> Self {
        Self { key, source }
    }

    pub(super) const fn observation(&self) -> JoinGroupCompletionObservation {
        JoinGroupCompletionObservation {
            key: self.key,
            source: self.source,
        }
    }

    pub(super) const fn key(&self) -> JoinGroupCallKey {
        self.key
    }

    pub(crate) fn into_parts(self) -> (JoinGroupCallKey, CompletionError) {
        (self.key, self.source)
    }
}

/// Accepted Join ownership recovered only after driver shutdown.
#[must_use = "recovered JoinGroup ownership still requires semantic settlement"]
pub(crate) struct RecoveredJoinGroupCall {
    key: JoinGroupCallKey,
}

impl RecoveredJoinGroupCall {
    pub(super) const fn new(key: JoinGroupCallKey) -> Self {
        Self { key }
    }

    pub(crate) const fn key(&self) -> JoinGroupCallKey {
        self.key
    }
}
