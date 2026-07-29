//! Linear ownership of one accepted group-wide `OffsetFetch` call.

mod evidence;

use std::time::Instant;

use kafka_client_core::ListConsumerGroupOffsetsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::OffsetFetchResponse;

use crate::protocol::admin::group_offsets::group_offsets_request;

use super::{
    super::DriverOwner,
    group_offsets_submission::GroupOffsetsSubmitError,
    group_offsets_terminal::{
        GroupOffsetsTerminal, RecoveredGroupOffsetsCall, retain_group_offsets_terminal,
    },
};

pub(super) use evidence::GroupOffsetsEvidence;

/// One accepted driver call retained beside its future admin operation owner.
#[must_use = "an accepted group-offset call must be terminally settled"]
pub(crate) struct GroupOffsetsCall {
    call: Option<RoutedCall<OffsetFetchResponse>>,
    evidence: Option<GroupOffsetsEvidence>,
}

impl GroupOffsetsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: ListConsumerGroupOffsetsPlan,
        result_limit: usize,
        deadline: Instant,
    ) -> Result<Self, GroupOffsetsCallAdmissionFailure> {
        let evidence = GroupOffsetsEvidence::new(plan, result_limit);
        let request = group_offsets_request(
            evidence.plan().group_id(),
            evidence.plan().selection(),
            evidence.plan().require_stable(),
        );
        let call = match driver.submit_tracked_group_offsets(
            evidence.plan().group_id(),
            request,
            deadline,
            evidence.plan().require_stable(),
        ) {
            Ok(call) => call,
            Err(source) => return Err(GroupOffsetsCallAdmissionFailure::new(source, evidence)),
        };
        Ok(Self {
            call: Some(call),
            evidence: Some(evidence),
        })
    }

    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(&mut self) -> Option<Result<GroupOffsetsTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_group_offsets_terminal(
                    selected_version,
                    result,
                    route_token,
                    evidence,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn matches_evidence(
        &self,
        plan: &ListConsumerGroupOffsetsPlan,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.matches(plan, result_limit))
    }

    /// Seals an unresolved accepted call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredGroupOffsetsCall> {
        let Self { call, evidence } = self;
        call.zip(evidence).map(|(call, evidence)| {
            drop(call);
            RecoveredGroupOffsetsCall::new(evidence)
        })
    }
}

/// Definitely-unsent rejection from coordinator validation or bounded driver admission.
#[must_use = "a rejected group-offset call must become an operation input"]
pub(crate) struct GroupOffsetsCallAdmissionFailure {
    source: GroupOffsetsSubmitError,
    evidence: GroupOffsetsEvidence,
}

impl GroupOffsetsCallAdmissionFailure {
    const fn new(source: GroupOffsetsSubmitError, evidence: GroupOffsetsEvidence) -> Self {
        Self { source, evidence }
    }

    pub(crate) fn into_submission_evidence(self) -> (ListConsumerGroupOffsetsPlan, usize) {
        let Self { source, evidence } = self;
        drop(source);
        evidence.into_parts()
    }
}
