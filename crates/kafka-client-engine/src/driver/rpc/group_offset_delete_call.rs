//! Linear ownership of one accepted group-coordinator `OffsetDelete` call.

mod evidence;

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::DeleteConsumerGroupOffsetsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::OffsetDeleteResponse;

use crate::protocol::admin::group_offset_delete::{
    GroupOffsetDeleteRequestFailure, OffsetDeleteTargetRef, group_offset_delete_request,
};

use super::{
    super::DriverOwner,
    group_offset_delete_submission::GroupOffsetDeleteSubmitError,
    group_offset_delete_terminal::{
        GroupOffsetDeleteTerminal, RecoveredGroupOffsetDeleteCall,
        retain_group_offset_delete_terminal,
    },
};

pub(super) use evidence::GroupOffsetDeleteEvidence;

/// One accepted driver call retained beside its future concrete operation owner.
#[must_use = "an accepted group-offset deletion call must be terminally settled"]
pub(crate) struct GroupOffsetDeleteCall {
    call: Option<RoutedCall<OffsetDeleteResponse>>,
    evidence: Option<GroupOffsetDeleteEvidence>,
}

impl GroupOffsetDeleteCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: DeleteConsumerGroupOffsetsPlan,
        result_limit: usize,
        deadline: Instant,
    ) -> Result<Self, GroupOffsetDeleteCallAdmissionFailure> {
        let evidence = GroupOffsetDeleteEvidence::new(plan, result_limit);
        let Some(targets) = request_targets(evidence.plan()) else {
            return Err(GroupOffsetDeleteCallAdmissionFailure::new(
                GroupOffsetDeleteAdmissionSource::Allocation,
                evidence,
            ));
        };
        let request = match group_offset_delete_request(
            evidence.plan().group_id(),
            &targets,
            evidence.result_limit(),
        ) {
            Ok(request) => request,
            Err(source) => {
                return Err(GroupOffsetDeleteCallAdmissionFailure::new(
                    GroupOffsetDeleteAdmissionSource::Request(source),
                    evidence,
                ));
            }
        };
        drop(targets);
        let call = match driver.submit_tracked_group_offset_delete(
            evidence.plan().group_id(),
            request,
            deadline,
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(GroupOffsetDeleteCallAdmissionFailure::new(
                    GroupOffsetDeleteAdmissionSource::Driver(source),
                    evidence,
                ));
            }
        };
        Ok(Self {
            call: Some(call),
            evidence: Some(evidence),
        })
    }

    /// Extracts a ready terminal once without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<GroupOffsetDeleteTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_group_offset_delete_terminal(
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
        plan: &DeleteConsumerGroupOffsetsPlan,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.matches(plan, result_limit))
    }

    /// Seals an unresolved accepted call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredGroupOffsetDeleteCall> {
        let Self { call, evidence } = self;
        call.zip(evidence).map(|(call, evidence)| {
            drop(call);
            RecoveredGroupOffsetDeleteCall::new(evidence)
        })
    }
}

#[derive(Debug)]
enum GroupOffsetDeleteAdmissionSource {
    Allocation,
    Request(GroupOffsetDeleteRequestFailure),
    Driver(GroupOffsetDeleteSubmitError),
}

/// Definitely-unsent rejection retaining the exact attempted deletion.
#[must_use = "a rejected group-offset deletion call must become an operation input"]
#[derive(Debug)]
pub(crate) struct GroupOffsetDeleteCallAdmissionFailure {
    source: GroupOffsetDeleteAdmissionSource,
    evidence: GroupOffsetDeleteEvidence,
}

impl GroupOffsetDeleteCallAdmissionFailure {
    const fn new(
        source: GroupOffsetDeleteAdmissionSource,
        evidence: GroupOffsetDeleteEvidence,
    ) -> Self {
        Self { source, evidence }
    }

    pub(crate) fn into_submission_evidence(self) -> (DeleteConsumerGroupOffsetsPlan, usize) {
        let Self { source, evidence } = self;
        drop(source);
        evidence.into_parts()
    }
}

impl fmt::Display for GroupOffsetDeleteCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            GroupOffsetDeleteAdmissionSource::Allocation => {
                formatter.write_str("OffsetDelete request-reference allocation failed")
            }
            GroupOffsetDeleteAdmissionSource::Request(source) => {
                write!(formatter, "OffsetDelete request rejected: {source}")
            }
            GroupOffsetDeleteAdmissionSource::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for GroupOffsetDeleteCallAdmissionFailure {}

fn request_targets(
    plan: &DeleteConsumerGroupOffsetsPlan,
) -> Option<Vec<OffsetDeleteTargetRef<'_>>> {
    let mut targets = Vec::new();
    targets.try_reserve_exact(plan.targets().len()).ok()?;
    targets.extend(
        plan.targets()
            .iter()
            .map(|target| OffsetDeleteTargetRef::new(target.topic(), target.partition())),
    );
    Some(targets)
}
