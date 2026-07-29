//! Linear ownership of one accepted AnyBroker SCRAM credential-alteration call.

use std::time::Instant;

use kafka_client_core::AlterUserScramCredentialsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::AlterUserScramCredentialsResponse;

use crate::protocol::admin::alter_user_scram_credentials::PreparedAlterUserScramCredentialsRequest;

use super::{
    super::DriverOwner,
    alter_user_scram_credentials_terminal::{
        AlterUserScramCredentialsRawTerminal, RecoveredAlterUserScramCredentialsCall,
        retain_alter_user_scram_credentials_terminal,
    },
};

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted AlterUserScramCredentials call must be terminally settled"]
pub(crate) struct AlterUserScramCredentialsCall {
    call: Option<RoutedCall<AlterUserScramCredentialsResponse>>,
    plan: Option<AlterUserScramCredentialsPlan>,
}

impl AlterUserScramCredentialsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: AlterUserScramCredentialsPlan,
        request: PreparedAlterUserScramCredentialsRequest,
        deadline: Instant,
    ) -> Result<Self, AlterUserScramCredentialsCallAdmissionFailure> {
        let call = driver
            .submit_alter_user_scram_credentials(request, deadline)
            .map_err(|_source| AlterUserScramCredentialsCallAdmissionFailure::Driver)?;
        Ok(Self {
            call: Some(call),
            plan: Some(plan),
        })
    }

    /// Extracts a ready raw terminal without blocking.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<AlterUserScramCredentialsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        drop(self.call.take());
        match result {
            Ok(outcome) => {
                let (result, selected_version, route_token) = outcome.into_parts();
                let plan = self.plan.take()?;
                Some(Ok(retain_alter_user_scram_credentials_terminal(
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
    ) -> Option<RecoveredAlterUserScramCredentialsCall> {
        self.call.take().map(|call| {
            drop(call);
            RecoveredAlterUserScramCredentialsCall::new(self.plan.take())
        })
    }
}

/// Definitely-unsent driver admission rejection.
#[must_use = "a rejected AlterUserScramCredentials call must become operation input"]
pub(crate) enum AlterUserScramCredentialsCallAdmissionFailure {
    Driver,
}
