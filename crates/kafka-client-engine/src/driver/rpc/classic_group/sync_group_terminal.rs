//! Raw `SyncGroup` terminal ownership retained for a later protocol interpreter.

use kafka_client_core::{GroupId, MembershipCycle};
use kafka_driver::{ApiVersion, CompletionError, RequestError};
use kafka_wire::SyncGroupResponse;

use crate::clock::OperationDeadline;

use super::super::sync_group_submission::SyncGroupSubmitError;

/// Exact correlation and deadline facts for one accepted Sync call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SyncGroupCallKey {
    group_id: GroupId,
    cycle: MembershipCycle,
    deadline: OperationDeadline,
}

impl SyncGroupCallKey {
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
#[must_use = "a raw SyncGroup terminal owns an unsettled membership cycle"]
pub(crate) struct SyncGroupTerminal {
    key: SyncGroupCallKey,
    selected_version: Option<i16>,
    result: Result<SyncGroupResponse, RequestError>,
}

impl SyncGroupTerminal {
    pub(crate) const fn key(&self) -> SyncGroupCallKey {
        self.key
    }

    pub(crate) const fn selected_version(&self) -> Option<i16> {
        self.selected_version
    }

    pub(crate) const fn result(&self) -> &Result<SyncGroupResponse, RequestError> {
        &self.result
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        SyncGroupCallKey,
        Option<i16>,
        Result<SyncGroupResponse, RequestError>,
    ) {
        (self.key, self.selected_version, self.result)
    }
}

pub(super) fn retain_sync_group_terminal(
    key: SyncGroupCallKey,
    selected_version: Option<ApiVersion>,
    result: Result<SyncGroupResponse, RequestError>,
) -> SyncGroupTerminal {
    SyncGroupTerminal {
        key,
        selected_version: selected_version.map(ApiVersion::value),
        result,
    }
}

/// Definitely-unsent driver rejection retaining exact cycle and deadline facts.
#[must_use = "a rejected SyncGroup call still owns its correlation facts"]
pub(crate) struct SyncGroupAdmissionFailure {
    key: SyncGroupCallKey,
    source: SyncGroupSubmitError,
}

impl SyncGroupAdmissionFailure {
    pub(super) const fn new(key: SyncGroupCallKey, source: SyncGroupSubmitError) -> Self {
        Self { key, source }
    }

    pub(crate) fn into_parts(self) -> (SyncGroupCallKey, SyncGroupSubmitError) {
        (self.key, self.source)
    }
}

/// Copyable observation while completion-corruption ownership stays retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SyncGroupCompletionObservation {
    key: SyncGroupCallKey,
    source: CompletionError,
}

impl SyncGroupCompletionObservation {
    pub(crate) const fn key(self) -> SyncGroupCallKey {
        self.key
    }

    pub(crate) const fn source(self) -> CompletionError {
        self.source
    }
}

/// Completion-cell failure retained until the driver owner is gone.
#[must_use = "completion failure retains one accepted SyncGroup cycle"]
pub(crate) struct SyncGroupCompletionFailure {
    key: SyncGroupCallKey,
    source: CompletionError,
}

impl SyncGroupCompletionFailure {
    pub(super) const fn new(key: SyncGroupCallKey, source: CompletionError) -> Self {
        Self { key, source }
    }

    pub(super) const fn observation(&self) -> SyncGroupCompletionObservation {
        SyncGroupCompletionObservation {
            key: self.key,
            source: self.source,
        }
    }

    pub(super) const fn key(&self) -> SyncGroupCallKey {
        self.key
    }

    pub(crate) fn into_parts(self) -> (SyncGroupCallKey, CompletionError) {
        (self.key, self.source)
    }
}

/// Accepted Sync ownership recovered only after driver shutdown.
#[must_use = "recovered SyncGroup ownership still requires semantic settlement"]
pub(crate) struct RecoveredSyncGroupCall {
    key: SyncGroupCallKey,
}

impl RecoveredSyncGroupCall {
    pub(super) const fn new(key: SyncGroupCallKey) -> Self {
        Self { key }
    }

    pub(crate) const fn key(&self) -> SyncGroupCallKey {
        self.key
    }
}
