//! Linear ownership of one accepted AnyBroker SCRAM credential-alteration call.

mod evidence;

use std::time::Instant;

use kafka_client_core::AlterUserScramCredentialsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::AlterUserScramCredentialsResponse;

use crate::protocol::admin::alter_user_scram_credentials::PreparedAlterUserScramCredentialsRequest;

use super::{
    super::DriverOwner,
    alter_user_scram_credentials_submission::AlterUserScramCredentialsSubmitError,
    alter_user_scram_credentials_terminal::{
        AlterUserScramCredentialsRawTerminal, RecoveredAlterUserScramCredentialsCall,
        retain_alter_user_scram_credentials_terminal,
    },
};

pub(super) use evidence::AlterUserScramCredentialsEvidence;

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted AlterUserScramCredentials call must be terminally settled"]
pub(crate) struct AlterUserScramCredentialsCall {
    call: Option<RoutedCall<AlterUserScramCredentialsResponse>>,
    evidence: Option<AlterUserScramCredentialsEvidence>,
}

impl AlterUserScramCredentialsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: AlterUserScramCredentialsPlan,
        request: PreparedAlterUserScramCredentialsRequest,
        result_limit: usize,
        deadline: Instant,
    ) -> Result<Self, AlterUserScramCredentialsCallAdmissionFailure> {
        let evidence = AlterUserScramCredentialsEvidence::new(
            plan,
            request.retained_heap_bytes(),
            result_limit,
        );
        if result_limit == 0 {
            drop(request);
            return Err(AlterUserScramCredentialsCallAdmissionFailure::new(
                AlterUserScramCredentialsAdmissionSource::Capacity,
                evidence,
            ));
        }
        let call = match driver.submit_alter_user_scram_credentials(request, deadline) {
            Ok(call) => call,
            Err(source) => {
                return Err(AlterUserScramCredentialsCallAdmissionFailure::new(
                    AlterUserScramCredentialsAdmissionSource::Driver(source),
                    evidence,
                ));
            }
        };
        Ok(Self {
            call: Some(call),
            evidence: Some(evidence),
        })
    }

    /// Extracts a ready raw terminal without blocking.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<AlterUserScramCredentialsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_alter_user_scram_credentials_terminal(
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
        plan: &AlterUserScramCredentialsPlan,
        prepared_request_bytes: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.matches(plan, prepared_request_bytes, result_limit))
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Result<RecoveredAlterUserScramCredentialsCall, Self> {
        if self.call.is_none() || self.evidence.is_none() {
            return Err(self);
        }
        let Self { call, evidence } = self;
        drop(call);
        Ok(RecoveredAlterUserScramCredentialsCall::new(
            evidence.unwrap_or_else(|| unreachable!("validated non-secret evidence")),
        ))
    }
}

#[derive(Debug)]
enum AlterUserScramCredentialsAdmissionSource {
    Capacity,
    Driver(AlterUserScramCredentialsSubmitError),
}

/// Definitely-unsent rejection retaining only non-secret attempt evidence.
#[derive(Debug)]
#[must_use = "a rejected AlterUserScramCredentials call must become operation input"]
pub(crate) struct AlterUserScramCredentialsCallAdmissionFailure {
    source: AlterUserScramCredentialsAdmissionSource,
    evidence: AlterUserScramCredentialsEvidence,
}

impl AlterUserScramCredentialsCallAdmissionFailure {
    const fn new(
        source: AlterUserScramCredentialsAdmissionSource,
        evidence: AlterUserScramCredentialsEvidence,
    ) -> Self {
        Self { source, evidence }
    }

    pub(crate) fn into_correlation(self) -> (AlterUserScramCredentialsPlan, usize, usize) {
        let Self { source, evidence } = self;
        match source {
            AlterUserScramCredentialsAdmissionSource::Capacity => {}
            AlterUserScramCredentialsAdmissionSource::Driver(source) => drop(source),
        }
        evidence.into_parts()
    }
}
