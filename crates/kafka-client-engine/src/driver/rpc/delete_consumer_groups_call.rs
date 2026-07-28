//! Linear ownership of one accepted coordinator-routed Admin `DeleteConsumerGroups` call.

mod evidence;

use std::time::Instant;

use kafka_client_core::{DeleteConsumerGroupsPlan, DeleteConsumerGroupsTarget};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DeleteGroupsResponse;

use crate::protocol::admin::delete_groups::delete_consumer_groups_request;

use super::{
    super::DriverOwner,
    delete_consumer_groups_terminal::{
        DeleteConsumerGroupsRawTerminal, RecoveredDeleteConsumerGroupsCall,
        retain_delete_consumer_groups_terminal,
    },
};

pub(super) use evidence::DeleteConsumerGroupsEvidence;

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted Admin DeleteConsumerGroups call must be terminally settled"]
pub(crate) struct DeleteConsumerGroupsCall {
    call: Option<RoutedCall<DeleteGroupsResponse>>,
    evidence: Option<DeleteConsumerGroupsEvidence>,
}

impl DeleteConsumerGroupsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: DeleteConsumerGroupsPlan,
        target: DeleteConsumerGroupsTarget,
        request_limit: usize,
        result_limit: usize,
        deadline: Instant,
    ) -> Result<Self, DeleteConsumerGroupsCallAdmissionFailure> {
        let evidence = DeleteConsumerGroupsEvidence::new(plan, target, request_limit, result_limit);
        let request = match delete_consumer_groups_request(evidence.target(), request_limit) {
            Ok(request) => request,
            Err(_source) => {
                return Err(DeleteConsumerGroupsCallAdmissionFailure::request(evidence));
            }
        };
        let call = match driver.submit_tracked_delete_consumer_groups(
            evidence.target().group_id(),
            request,
            deadline,
        ) {
            Ok(call) => call,
            Err(_source) => return Err(DeleteConsumerGroupsCallAdmissionFailure::driver(evidence)),
        };
        Ok(Self {
            call: Some(call),
            evidence: Some(evidence),
        })
    }

    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DeleteConsumerGroupsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_delete_consumer_groups_terminal(
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
        plan: &DeleteConsumerGroupsPlan,
        target: &DeleteConsumerGroupsTarget,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.matches(plan, target, request_limit, result_limit))
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDeleteConsumerGroupsCall> {
        let Self { call, evidence } = self;
        call.zip(evidence).map(|(call, evidence)| {
            drop(call);
            RecoveredDeleteConsumerGroupsCall::new(evidence)
        })
    }
}

/// Definitely-unsent failure from request construction or driver admission.
#[must_use = "a rejected Admin DeleteConsumerGroups call must become an operation input"]
pub(crate) struct DeleteConsumerGroupsCallAdmissionFailure {
    source: DeleteConsumerGroupsCallAdmissionFailureSource,
    evidence: DeleteConsumerGroupsEvidence,
}

enum DeleteConsumerGroupsCallAdmissionFailureSource {
    Request,
    Driver,
}

impl DeleteConsumerGroupsCallAdmissionFailure {
    const fn request(evidence: DeleteConsumerGroupsEvidence) -> Self {
        Self {
            source: DeleteConsumerGroupsCallAdmissionFailureSource::Request,
            evidence,
        }
    }

    const fn driver(evidence: DeleteConsumerGroupsEvidence) -> Self {
        Self {
            source: DeleteConsumerGroupsCallAdmissionFailureSource::Driver,
            evidence,
        }
    }

    pub(crate) fn into_submission_evidence(
        self,
    ) -> (
        DeleteConsumerGroupsPlan,
        DeleteConsumerGroupsTarget,
        usize,
        usize,
    ) {
        let Self { source, evidence } = self;
        let _ = source;
        evidence.into_parts()
    }
}
