//! Linear ownership of one accepted `AnyBroker` `DescribeAcls` call.

use std::time::Instant;

use kafka_client_core::DescribeAclsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeAclsResponse;

use crate::protocol::admin::describe_acls::{DescribeAclsFilterRef, describe_acls_request};

use super::{
    super::DriverOwner,
    describe_acls_terminal::{
        DescribeAclsRawTerminal, RecoveredDescribeAclsCall, retain_describe_acls_terminal,
    },
};

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeAcls call must be terminally settled"]
pub(crate) struct DescribeAclsCall {
    call: Option<RoutedCall<DescribeAclsResponse>>,
    plan: Option<DescribeAclsPlan>,
    result_limit: usize,
}

impl DescribeAclsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: DescribeAclsPlan,
        result_limit: usize,
        deadline: Instant,
    ) -> Result<Self, DescribeAclsCallAdmissionFailure> {
        let filter = plan.filter();
        let filter = DescribeAclsFilterRef::new(
            filter.resource_type(),
            filter.resource_name(),
            filter.pattern_type(),
            filter.principal(),
            filter.host(),
            filter.operation(),
            filter.permission_type(),
        );
        let request = match describe_acls_request(filter, result_limit) {
            Ok(request) => request,
            Err(_source) => {
                return Err(DescribeAclsCallAdmissionFailure::new(
                    DescribeAclsCallAdmissionSource::Request,
                    plan,
                    result_limit,
                ));
            }
        };
        let call = match driver.submit_describe_acls(request, deadline) {
            Ok(call) => call,
            Err(_source) => {
                return Err(DescribeAclsCallAdmissionFailure::new(
                    DescribeAclsCallAdmissionSource::Driver,
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

    /// Extracts a ready raw terminal without blocking.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeAclsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let plan = self.plan.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_acls_terminal(
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

    pub(crate) fn matches(&self, plan: &DescribeAclsPlan, result_limit: usize) -> bool {
        self.plan
            .as_ref()
            .is_some_and(|owned| owned == plan && self.result_limit == result_limit)
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDescribeAclsCall> {
        let Self {
            call,
            plan,
            result_limit,
        } = self;
        call.zip(plan).map(|(call, plan)| {
            drop(call);
            RecoveredDescribeAclsCall::new(plan, result_limit)
        })
    }
}

/// Definitely-unsent bounded-driver rejection.
#[must_use = "a rejected DescribeAcls call must become operation input"]
enum DescribeAclsCallAdmissionSource {
    Request,
    Driver,
}

/// Exact query correlation returned when no tracked driver call was accepted.
#[must_use = "a rejected DescribeAcls call must become operation input"]
pub(crate) struct DescribeAclsCallAdmissionFailure {
    source: DescribeAclsCallAdmissionSource,
    plan: DescribeAclsPlan,
    result_limit: usize,
}

impl DescribeAclsCallAdmissionFailure {
    const fn new(
        source: DescribeAclsCallAdmissionSource,
        plan: DescribeAclsPlan,
        result_limit: usize,
    ) -> Self {
        Self {
            source,
            plan,
            result_limit,
        }
    }

    pub(crate) fn into_correlation(self) -> (DescribeAclsPlan, usize) {
        let Self {
            source,
            plan,
            result_limit,
        } = self;
        let _ = source;
        (plan, result_limit)
    }
}
