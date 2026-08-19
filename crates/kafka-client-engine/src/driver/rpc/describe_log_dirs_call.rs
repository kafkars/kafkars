//! Linear ownership of one accepted exact-broker `DescribeLogDirs` call.

pub(super) mod evidence;

use std::time::Instant;

use kafka_client_core::AdminDescribeLogDirsSelection;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeLogDirsResponse;

use crate::protocol::admin::describe_log_dirs::describe_log_dirs_request_for_selection;

use super::{
    super::DriverOwner,
    describe_log_dirs_terminal::{
        DescribeLogDirsRawTerminal, RecoveredDescribeLogDirsCall, retain_describe_log_dirs_terminal,
    },
};
use evidence::DescribeLogDirsEvidence;

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeLogDirs call must be terminally settled"]
pub(crate) struct DescribeLogDirsCall {
    call: Option<RoutedCall<DescribeLogDirsResponse>>,
    evidence: Option<DescribeLogDirsEvidence>,
}

impl DescribeLogDirsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        broker_id: i32,
        selection: AdminDescribeLogDirsSelection,
        request_scratch_limit: usize,
        result_limit: usize,
        deadline: Instant,
    ) -> Result<Self, DescribeLogDirsCallAdmissionFailure> {
        let evidence =
            DescribeLogDirsEvidence::new(broker_id, selection, request_scratch_limit, result_limit);
        let request = match describe_log_dirs_request_for_selection(
            evidence.selection(),
            request_scratch_limit,
        ) {
            Ok(request) => request,
            Err(_source) => return Err(DescribeLogDirsCallAdmissionFailure::request(evidence)),
        };
        let call = match driver.submit_tracked_describe_log_dirs(broker_id, request, deadline) {
            Ok(call) => call,
            Err(_source) => return Err(DescribeLogDirsCallAdmissionFailure::submit(evidence)),
        };
        Ok(Self {
            call: Some(call),
            evidence: Some(evidence),
        })
    }

    /// Extracts a ready raw terminal without blocking or losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeLogDirsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_log_dirs_terminal(
                    evidence,
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn matches(
        &self,
        broker_id: i32,
        selection: &AdminDescribeLogDirsSelection,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence.as_ref().is_some_and(|evidence| {
            evidence.matches(broker_id, selection, request_scratch_limit, result_limit)
        })
    }

    /// Seals unresolved ownership only after the unique driver is destroyed.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDescribeLogDirsCall> {
        self.call.zip(self.evidence).map(|(call, evidence)| {
            drop(call);
            RecoveredDescribeLogDirsCall::new(evidence)
        })
    }
}

/// Definitely-unsent route validation or bounded-driver rejection.
#[must_use = "a rejected DescribeLogDirs call must become operation input"]
enum DescribeLogDirsCallAdmissionSource {
    Request,
    Submit,
}

/// Exact evidence returned when no tracked call was accepted.
#[must_use = "a rejected DescribeLogDirs call must become operation input"]
pub(crate) struct DescribeLogDirsCallAdmissionFailure {
    source: DescribeLogDirsCallAdmissionSource,
    evidence: DescribeLogDirsEvidence,
}

impl DescribeLogDirsCallAdmissionFailure {
    const fn request(evidence: DescribeLogDirsEvidence) -> Self {
        Self {
            source: DescribeLogDirsCallAdmissionSource::Request,
            evidence,
        }
    }

    const fn submit(evidence: DescribeLogDirsEvidence) -> Self {
        Self {
            source: DescribeLogDirsCallAdmissionSource::Submit,
            evidence,
        }
    }

    pub(crate) fn into_correlation(self) -> (i32, AdminDescribeLogDirsSelection, usize, usize) {
        let Self { source, evidence } = self;
        let _ = source;
        evidence.into_parts()
    }
}
