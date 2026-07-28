//! Linear ownership of one accepted AnyBroker SCRAM credential-description call.

use std::time::Instant;

use kafka_client_core::DescribeUserScramCredentialsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DescribeUserScramCredentialsResponse;

use crate::protocol::admin::describe_user_scram_credentials::{
    DescribeUserScramCredentialsRequestRef, describe_user_scram_credentials_request,
};

use super::{
    super::DriverOwner,
    describe_user_scram_credentials_terminal::{
        DescribeUserScramCredentialsRawTerminal, RecoveredDescribeUserScramCredentialsCall,
        retain_describe_user_scram_credentials_terminal,
    },
};

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeUserScramCredentials call must be terminally settled"]
pub(crate) struct DescribeUserScramCredentialsCall {
    call: Option<RoutedCall<DescribeUserScramCredentialsResponse>>,
    plan: Option<DescribeUserScramCredentialsPlan>,
}

impl DescribeUserScramCredentialsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: DescribeUserScramCredentialsPlan,
        retained_limit: usize,
        deadline: Instant,
    ) -> Result<Self, DescribeUserScramCredentialsCallAdmissionFailure> {
        let selection = request_ref(&plan);
        let request = describe_user_scram_credentials_request(selection, retained_limit)
            .map_err(|_source| DescribeUserScramCredentialsCallAdmissionFailure::Request)?;
        let call = driver
            .submit_describe_user_scram_credentials(request, deadline)
            .map_err(|_source| DescribeUserScramCredentialsCallAdmissionFailure::Driver)?;
        Ok(Self {
            call: Some(call),
            plan: Some(plan),
        })
    }

    /// Extracts a ready raw terminal without blocking.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DescribeUserScramCredentialsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                let plan = self.plan.take()?;
                Some(Ok(retain_describe_user_scram_credentials_terminal(
                    selected_version,
                    result,
                    route_token,
                    plan,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(
        mut self,
    ) -> Option<RecoveredDescribeUserScramCredentialsCall> {
        self.call.take().map(|call| {
            drop(call);
            RecoveredDescribeUserScramCredentialsCall::new(self.plan.take())
        })
    }
}

fn request_ref(
    plan: &DescribeUserScramCredentialsPlan,
) -> DescribeUserScramCredentialsRequestRef<'_> {
    match plan.users() {
        Some(users) => DescribeUserScramCredentialsRequestRef::selected(users),
        None => DescribeUserScramCredentialsRequestRef::all(),
    }
}

/// Definitely-unsent bounded-driver rejection.
#[must_use = "a rejected DescribeUserScramCredentials call must become operation input"]
pub(crate) enum DescribeUserScramCredentialsCallAdmissionFailure {
    Request,
    Driver,
}
