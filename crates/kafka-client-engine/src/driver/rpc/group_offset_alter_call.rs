//! Linear ownership of one accepted group-coordinator `OffsetCommit` call.

mod evidence;

use std::{error::Error, fmt, time::Instant};

use kafka_client_core::AlterConsumerGroupOffsetsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::OffsetCommitResponse;

use crate::protocol::admin::group_offset_alter::{
    GroupOffsetAlterRequestFailure, OffsetCommitTargetRef, group_offset_alter_request,
};

use super::{
    super::DriverOwner,
    group_offset_alter_submission::GroupOffsetAlterSubmitError,
    group_offset_alter_terminal::{
        GroupOffsetAlterTerminal, RecoveredGroupOffsetAlterCall, retain_group_offset_alter_terminal,
    },
};

pub(super) use evidence::GroupOffsetAlterEvidence;

/// One accepted driver call retained beside its future concrete operation owner.
#[must_use = "an accepted group-offset alteration call must be terminally settled"]
pub(crate) struct GroupOffsetAlterCall {
    call: Option<RoutedCall<OffsetCommitResponse>>,
    evidence: Option<GroupOffsetAlterEvidence>,
}

impl GroupOffsetAlterCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
        deadline: Instant,
    ) -> Result<Self, GroupOffsetAlterCallAdmissionFailure> {
        let evidence = GroupOffsetAlterEvidence::new(plan, request_scratch_limit, result_limit);
        if result_limit == 0 {
            return Err(GroupOffsetAlterCallAdmissionFailure::new(
                GroupOffsetAlterAdmissionSource::Capacity,
                evidence,
            ));
        }
        let Some(targets) = request_targets(evidence.plan()) else {
            return Err(GroupOffsetAlterCallAdmissionFailure::new(
                GroupOffsetAlterAdmissionSource::Allocation,
                evidence,
            ));
        };
        let request = match group_offset_alter_request(
            evidence.plan().group_id(),
            &targets,
            evidence.plan().retention_time_ms(),
            evidence.request_scratch_limit(),
        ) {
            Ok(request) => request,
            Err(source) => {
                return Err(GroupOffsetAlterCallAdmissionFailure::new(
                    GroupOffsetAlterAdmissionSource::Request(source),
                    evidence,
                ));
            }
        };
        let call = match driver.submit_tracked_group_offset_alter(
            evidence.plan().group_id(),
            &targets,
            evidence.plan().retention_time_ms(),
            request,
            deadline,
        ) {
            Ok(call) => call,
            Err(source) => {
                return Err(GroupOffsetAlterCallAdmissionFailure::new(
                    GroupOffsetAlterAdmissionSource::Driver(source),
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
    ) -> Option<Result<GroupOffsetAlterTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_group_offset_alter_terminal(
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
        plan: &AlterConsumerGroupOffsetsPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.matches(plan, request_scratch_limit, result_limit))
    }

    /// Seals an unresolved accepted call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Result<RecoveredGroupOffsetAlterCall, Self> {
        if self.call.is_none() || self.evidence.is_none() {
            return Err(self);
        }
        let Self { call, evidence } = self;
        drop(call);
        Ok(RecoveredGroupOffsetAlterCall::new(evidence.unwrap_or_else(
            || unreachable!("validated OffsetCommit evidence"),
        )))
    }
}

#[derive(Debug)]
enum GroupOffsetAlterAdmissionSource {
    Capacity,
    Allocation,
    Request(GroupOffsetAlterRequestFailure),
    Driver(GroupOffsetAlterSubmitError),
}

/// Definitely-unsent rejection from coordinator validation or bounded driver admission.
#[must_use = "a rejected group-offset alteration call must become an operation input"]
#[derive(Debug)]
pub(crate) struct GroupOffsetAlterCallAdmissionFailure {
    source: GroupOffsetAlterAdmissionSource,
    evidence: GroupOffsetAlterEvidence,
}

impl GroupOffsetAlterCallAdmissionFailure {
    const fn new(
        source: GroupOffsetAlterAdmissionSource,
        evidence: GroupOffsetAlterEvidence,
    ) -> Self {
        Self { source, evidence }
    }

    pub(crate) fn into_submission_evidence(self) -> (AlterConsumerGroupOffsetsPlan, usize, usize) {
        let Self { source, evidence } = self;
        drop(source);
        evidence.into_parts()
    }
}

impl fmt::Display for GroupOffsetAlterCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            GroupOffsetAlterAdmissionSource::Capacity => {
                formatter.write_str("OffsetCommit result capacity is empty")
            }
            GroupOffsetAlterAdmissionSource::Allocation => {
                formatter.write_str("OffsetCommit request-reference allocation failed")
            }
            GroupOffsetAlterAdmissionSource::Request(source) => {
                write!(formatter, "OffsetCommit request rejected: {source}")
            }
            GroupOffsetAlterAdmissionSource::Driver(source) => write!(formatter, "{source}"),
        }
    }
}

impl Error for GroupOffsetAlterCallAdmissionFailure {}

fn request_targets(plan: &AlterConsumerGroupOffsetsPlan) -> Option<Vec<OffsetCommitTargetRef<'_>>> {
    let mut targets = Vec::new();
    targets.try_reserve_exact(plan.targets().len()).ok()?;
    targets.extend(plan.targets().iter().map(|target| {
        OffsetCommitTargetRef::new(
            target.topic(),
            target.partition(),
            target.next_offset(),
            target.leader_epoch(),
            target.metadata(),
        )
    }));
    Some(targets)
}
