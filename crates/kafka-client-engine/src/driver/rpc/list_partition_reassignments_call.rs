//! Linear ownership of one exact accepted controller reassignment-listing call.

use std::{error::Error, fmt};

use kafka_client_core::{ListPartitionReassignmentsPlan, Moment};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::ListPartitionReassignmentsResponse;

use crate::{
    clock::OperationDeadline,
    protocol::admin::list_partition_reassignments::{
        ListPartitionReassignmentsRequestError, list_partition_reassignments_request,
        remaining_timeout_ms,
    },
};

use super::{
    super::DriverOwner,
    list_partition_reassignments_submission::ListPartitionReassignmentsSubmitError,
    list_partition_reassignments_terminal::{
        ListPartitionReassignmentsRawTerminal, RecoveredListPartitionReassignmentsCall,
        retain_list_partition_reassignments_terminal,
    },
};

/// One accepted driver call retained beside its concrete Admin operation.
#[must_use = "an accepted reassignment-listing call must be terminally settled"]
pub(crate) struct ListPartitionReassignmentsCall {
    call: Option<RoutedCall<ListPartitionReassignmentsResponse>>,
    plan: Option<ListPartitionReassignmentsPlan>,
    result_limit: usize,
}

impl ListPartitionReassignmentsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: ListPartitionReassignmentsPlan,
        result_limit: usize,
        now: Moment,
        deadline: OperationDeadline,
    ) -> Result<Self, ListPartitionReassignmentsCallAdmissionFailure> {
        let timeout_ms = match remaining_timeout_ms(now, deadline.core()) {
            Ok(timeout_ms) => timeout_ms,
            Err(source) => {
                return Err(ListPartitionReassignmentsCallAdmissionFailure::deadline(
                    source,
                    plan,
                    result_limit,
                ));
            }
        };
        let request = list_partition_reassignments_request(&plan, timeout_ms);
        let call = match driver
            .submit_tracked_list_partition_reassignments(request, deadline.transport())
        {
            Ok(call) => call,
            Err(source) => {
                return Err(ListPartitionReassignmentsCallAdmissionFailure::driver(
                    source,
                    plan,
                    result_limit,
                ));
            }
        };
        Ok(Self {
            call: Some(call),
            plan: Some(plan),
            result_limit,
        })
    }

    /// Extracts a ready raw terminal without losing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<ListPartitionReassignmentsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let plan = self.plan.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_list_partition_reassignments_terminal(
                    plan,
                    self.result_limit,
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
        expected_plan: &ListPartitionReassignmentsPlan,
        expected_result_limit: usize,
    ) -> bool {
        self.plan
            .as_ref()
            .is_some_and(|plan| plan == expected_plan && self.result_limit == expected_result_limit)
    }

    /// Seals unresolved accepted ownership only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Result<RecoveredListPartitionReassignmentsCall, Self> {
        if self.call.is_none() || self.plan.is_none() {
            return Err(self);
        }
        let Self {
            call,
            plan,
            result_limit,
        } = self;
        drop(call);
        Ok(RecoveredListPartitionReassignmentsCall::new(
            plan.unwrap_or_else(|| unreachable!("validated exact plan")),
            result_limit,
        ))
    }
}

/// Definitely-unsent rejection from deadline or bounded driver admission.
#[must_use = "a rejected reassignment-listing call must become an operation input"]
#[derive(Debug)]
pub(crate) struct ListPartitionReassignmentsCallAdmissionFailure {
    source: ListPartitionReassignmentsCallAdmissionSource,
    plan: ListPartitionReassignmentsPlan,
    result_limit: usize,
}

#[derive(Debug)]
enum ListPartitionReassignmentsCallAdmissionSource {
    Deadline(ListPartitionReassignmentsRequestError),
    Driver(ListPartitionReassignmentsSubmitError),
}

impl ListPartitionReassignmentsCallAdmissionFailure {
    const fn deadline(
        source: ListPartitionReassignmentsRequestError,
        plan: ListPartitionReassignmentsPlan,
        result_limit: usize,
    ) -> Self {
        Self {
            source: ListPartitionReassignmentsCallAdmissionSource::Deadline(source),
            plan,
            result_limit,
        }
    }

    const fn driver(
        source: ListPartitionReassignmentsSubmitError,
        plan: ListPartitionReassignmentsPlan,
        result_limit: usize,
    ) -> Self {
        Self {
            source: ListPartitionReassignmentsCallAdmissionSource::Driver(source),
            plan,
            result_limit,
        }
    }

    pub(crate) fn into_correlation(self) -> (ListPartitionReassignmentsPlan, usize) {
        let Self {
            source,
            plan,
            result_limit,
        } = self;
        match source {
            ListPartitionReassignmentsCallAdmissionSource::Deadline(source) => {
                let _ = source;
            }
            ListPartitionReassignmentsCallAdmissionSource::Driver(source) => drop(source),
        }
        (plan, result_limit)
    }
}

impl fmt::Display for ListPartitionReassignmentsCallAdmissionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            ListPartitionReassignmentsCallAdmissionSource::Deadline(source) => {
                write!(formatter, "request deadline rejected: {source:?}")
            }
            ListPartitionReassignmentsCallAdmissionSource::Driver(source) => {
                write!(formatter, "{source}")
            }
        }
    }
}

impl Error for ListPartitionReassignmentsCallAdmissionFailure {}
