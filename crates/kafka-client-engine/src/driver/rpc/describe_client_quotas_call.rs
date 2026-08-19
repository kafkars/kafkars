//! Linear ownership of one accepted `AnyBroker` `DescribeClientQuotas` call.

use std::time::Instant;

use kafka_client_core::{
    ClientQuotaMatch, DescribeClientQuotaFilterComponent, DescribeClientQuotasPlan,
};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeClientQuotasResponse;

use crate::protocol::admin::describe_client_quotas::{
    DescribeClientQuotaFilterComponentRef, DescribeClientQuotaMatchRef,
    DescribeClientQuotasFilterRef, describe_client_quotas_request,
};

use super::{
    super::DriverOwner,
    describe_client_quotas_terminal::{
        DescribeClientQuotasRawTerminal, RecoveredDescribeClientQuotasCall,
        retain_describe_client_quotas_terminal,
    },
};

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeClientQuotas call must be terminally settled"]
pub(crate) struct DescribeClientQuotasCall {
    call: Option<RoutedCall<DescribeClientQuotasResponse>>,
    plan: Option<DescribeClientQuotasPlan>,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl DescribeClientQuotasCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: DescribeClientQuotasPlan,
        request_scratch_limit: usize,
        result_limit: usize,
        deadline: Instant,
    ) -> Result<Self, DescribeClientQuotasCallAdmissionFailure> {
        let mut refs = Vec::new();
        if refs.try_reserve_exact(plan.components().len()).is_err() {
            return Err(DescribeClientQuotasCallAdmissionFailure::request(
                plan,
                request_scratch_limit,
                result_limit,
            ));
        }
        refs.extend(plan.components().iter().map(component_ref));
        let filter = DescribeClientQuotasFilterRef::new(&refs, plan.strict());
        let request = describe_client_quotas_request(filter, request_scratch_limit);
        drop(refs);
        let request = match request {
            Ok(request) => request,
            Err(_source) => {
                return Err(DescribeClientQuotasCallAdmissionFailure::request(
                    plan,
                    request_scratch_limit,
                    result_limit,
                ));
            }
        };
        let call = match driver.submit_describe_client_quotas(request, deadline) {
            Ok(call) => call,
            Err(_source) => {
                return Err(DescribeClientQuotasCallAdmissionFailure::driver(
                    plan,
                    request_scratch_limit,
                    result_limit,
                ));
            }
        };
        Ok(Self {
            call: Some(call),
            plan: Some(plan),
            request_scratch_limit,
            result_limit,
        })
    }

    /// Extracts a ready raw terminal without blocking.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeClientQuotasRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let plan = self.plan.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_client_quotas_terminal(
                    selected_version,
                    result,
                    route_token,
                    plan,
                    self.request_scratch_limit,
                    self.result_limit,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn matches(
        &self,
        plan: &DescribeClientQuotasPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.plan.as_ref().is_some_and(|owned| {
            owned == plan
                && self.request_scratch_limit == request_scratch_limit
                && self.result_limit == result_limit
        })
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDescribeClientQuotasCall> {
        let Self {
            call,
            plan,
            request_scratch_limit,
            result_limit,
        } = self;
        call.zip(plan).map(|(call, plan)| {
            drop(call);
            RecoveredDescribeClientQuotasCall::new(plan, request_scratch_limit, result_limit)
        })
    }
}

fn component_ref(
    component: &DescribeClientQuotaFilterComponent,
) -> DescribeClientQuotaFilterComponentRef<'_> {
    let match_ = match component.match_kind() {
        ClientQuotaMatch::Exact(name) => DescribeClientQuotaMatchRef::Exact(name),
        ClientQuotaMatch::Default => DescribeClientQuotaMatchRef::Default,
        ClientQuotaMatch::AnySpecified => DescribeClientQuotaMatchRef::AnySpecified,
    };
    DescribeClientQuotaFilterComponentRef::new(component.entity_type(), match_)
}

/// Definitely-unsent bounded-driver rejection.
#[must_use = "a rejected DescribeClientQuotas call must become operation input"]
enum DescribeClientQuotasCallAdmissionSource {
    Request,
    Driver,
}

/// Exact query evidence returned when the driver accepted no tracked call.
#[must_use = "a rejected DescribeClientQuotas call must become operation input"]
pub(crate) struct DescribeClientQuotasCallAdmissionFailure {
    source: DescribeClientQuotasCallAdmissionSource,
    plan: DescribeClientQuotasPlan,
    request_scratch_limit: usize,
    result_limit: usize,
}

impl DescribeClientQuotasCallAdmissionFailure {
    const fn request(
        plan: DescribeClientQuotasPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            source: DescribeClientQuotasCallAdmissionSource::Request,
            plan,
            request_scratch_limit,
            result_limit,
        }
    }

    const fn driver(
        plan: DescribeClientQuotasPlan,
        request_scratch_limit: usize,
        result_limit: usize,
    ) -> Self {
        Self {
            source: DescribeClientQuotasCallAdmissionSource::Driver,
            plan,
            request_scratch_limit,
            result_limit,
        }
    }

    pub(crate) fn into_correlation(self) -> (DescribeClientQuotasPlan, usize, usize) {
        let Self {
            source,
            plan,
            request_scratch_limit,
            result_limit,
        } = self;
        let _ = source;
        (plan, request_scratch_limit, result_limit)
    }
}
