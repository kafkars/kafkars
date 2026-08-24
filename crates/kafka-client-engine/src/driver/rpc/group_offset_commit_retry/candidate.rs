//! One exact same-deadline group commit replacement after coordinator invalidation.

use kafka_client_core::{DeliveryStatus, GroupOffsetCommitInput, OperationId};
use kafka_driver::{ApiVersion, RequestError, RouteFailureToken};
use kafka_wire::OffsetCommitResponse;

use crate::protocol::consumer::{
    PreparedGroupOffsetCommit,
    group_offset_commit::is_exact_group_offset_commit_coordinator_rejection,
};

use super::super::{
    super::DriverOwner, group_offset_commit_calls::TrackedGroupOffsetCommitCalls,
    group_offset_commit_settlement::SettledGroupOffsetCommitCall,
    group_offset_commit_terminal::normalize_group_offset_commit_terminal,
};

/// Linear ownership of the sole coordinator-rejected replacement candidate.
#[must_use = "a group commit retry candidate must be replaced or terminally settled"]
pub(in crate::driver::rpc) struct GroupOffsetCommitRetryCandidate {
    prepared: PreparedGroupOffsetCommit,
    version: ApiVersion,
    response: OffsetCommitResponse,
}

impl GroupOffsetCommitRetryCandidate {
    #[allow(
        clippy::result_large_err,
        reason = "classification failure returns both exact linear owners"
    )]
    pub(in crate::driver::rpc) fn try_new(
        prepared: PreparedGroupOffsetCommit,
        selected_version: Option<ApiVersion>,
        response: OffsetCommitResponse,
    ) -> Result<Self, (PreparedGroupOffsetCommit, OffsetCommitResponse)> {
        let Some(version) = selected_version else {
            return Err((prepared, response));
        };
        let compatible = (2..=9).contains(&version.value())
            && (!prepared.requires_leader_epoch() || version.value() >= 6)
            && (!prepared.requires_consumer_group_version() || version.value() >= 9);
        if !compatible || !is_exact_group_offset_commit_coordinator_rejection(&prepared, &response)
        {
            return Err((prepared, response));
        }
        Ok(Self {
            prepared,
            version,
            response,
        })
    }

    pub(in crate::driver::rpc) const fn operation_id(&self) -> OperationId {
        self.prepared.operation_id()
    }

    pub(in crate::driver::rpc) fn into_prepared(self) -> PreparedGroupOffsetCommit {
        self.prepared
    }

    pub(in crate::driver::rpc) fn into_terminal(self) -> GroupOffsetCommitInput {
        normalize_group_offset_commit_terminal(self.prepared, Some(self.version), Ok(self.response))
    }
}

/// One bounded replacement-submission result for an already accepted commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupOffsetCommitReplacementPoll {
    Submitted,
    Settled,
}

pub(super) const fn replacement_admission_terminal(
    preparation_failed: bool,
) -> GroupOffsetCommitInput {
    if preparation_failed {
        GroupOffsetCommitInput::ExecutionUnavailable
    } else {
        GroupOffsetCommitInput::TransportFailed {
            delivery: DeliveryStatus::NotSent,
        }
    }
}

pub(in crate::driver::rpc) fn classify_group_offset_commit_settlement(
    prepared: PreparedGroupOffsetCommit,
    selected_version: Option<ApiVersion>,
    result: Result<OffsetCommitResponse, RequestError>,
    route_token: Option<RouteFailureToken>,
    replacement_used: bool,
) -> SettledGroupOffsetCommitCall {
    let operation_id = prepared.operation_id();
    match (replacement_used, route_token, result) {
        (false, Some(route_token), Ok(response)) => {
            match GroupOffsetCommitRetryCandidate::try_new(prepared, selected_version, response) {
                Ok(candidate) => SettledGroupOffsetCommitCall::new_retry(candidate, route_token),
                Err((prepared, response)) => SettledGroupOffsetCommitCall::new(
                    operation_id,
                    normalize_group_offset_commit_terminal(
                        prepared,
                        selected_version,
                        Ok(response),
                    ),
                    Some(route_token),
                ),
            }
        }
        (_replacement_used, route_token, result) => SettledGroupOffsetCommitCall::new(
            operation_id,
            normalize_group_offset_commit_terminal(prepared, selected_version, result),
            route_token,
        ),
    }
}

impl TrackedGroupOffsetCommitCalls {
    pub(crate) fn submit_group_commit_replacement(
        &mut self,
        operation_id: OperationId,
        driver: &DriverOwner,
    ) -> Option<GroupOffsetCommitReplacementPoll> {
        let ready = self.settled.as_ref().is_some_and(|settled| {
            settled.operation_id() == operation_id && settled.is_retry_ready()
        });
        if !ready {
            return None;
        }
        let candidate = self.settled.take()?.into_retry_candidate()?;
        let prepared = candidate.into_prepared();
        let request =
            match crate::protocol::consumer::PreparedGroupOffsetCommitRequest::try_from_prepared(
                &prepared,
            ) {
                Ok(request) => request,
                Err(_error) => {
                    self.settled = Some(SettledGroupOffsetCommitCall::new(
                        operation_id,
                        replacement_admission_terminal(true),
                        None,
                    ));
                    return Some(GroupOffsetCommitReplacementPoll::Settled);
                }
            };
        let Some(permit) = self.try_reserve_group_commit() else {
            self.settled = Some(SettledGroupOffsetCommitCall::new(
                operation_id,
                replacement_admission_terminal(true),
                None,
            ));
            return Some(GroupOffsetCommitReplacementPoll::Settled);
        };
        match permit.submit_replacement(driver, prepared, request) {
            Ok(()) => Some(GroupOffsetCommitReplacementPoll::Submitted),
            Err(failure) => {
                let (_prepared, _input, _source) = failure.into_parts();
                self.settled = Some(SettledGroupOffsetCommitCall::new(
                    operation_id,
                    replacement_admission_terminal(false),
                    None,
                ));
                Some(GroupOffsetCommitReplacementPoll::Settled)
            }
        }
    }
}
