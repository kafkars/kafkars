//! Linear ownership of one accepted AnyBroker `DescribeClientQuotas` call.

use std::time::Instant;

use kafka_client_core::{ClientQuotaMatch, DescribeClientQuotaFilterComponent};
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
}

impl DescribeClientQuotasCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        components: &[DescribeClientQuotaFilterComponent],
        strict: bool,
        retained_limit: usize,
        deadline: Instant,
    ) -> Result<Self, DescribeClientQuotasCallAdmissionFailure> {
        let mut refs = Vec::new();
        refs.try_reserve_exact(components.len())
            .map_err(|_error| DescribeClientQuotasCallAdmissionFailure::Request)?;
        refs.extend(components.iter().map(component_ref));
        let filter = DescribeClientQuotasFilterRef::new(&refs, strict);
        let request = describe_client_quotas_request(filter, retained_limit)
            .map_err(|_source| DescribeClientQuotasCallAdmissionFailure::Request)?;
        drop(refs);
        let call = driver
            .submit_describe_client_quotas(request, deadline)
            .map_err(|_source| DescribeClientQuotasCallAdmissionFailure::Driver)?;
        Ok(Self { call: Some(call) })
    }

    /// Extracts a ready raw terminal without blocking.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeClientQuotasRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_client_quotas_terminal(
                    selected_version,
                    result,
                    route_token,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDescribeClientQuotasCall> {
        self.call.map(|call| {
            drop(call);
            RecoveredDescribeClientQuotasCall::new()
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
pub(crate) enum DescribeClientQuotasCallAdmissionFailure {
    Request,
    Driver,
}
