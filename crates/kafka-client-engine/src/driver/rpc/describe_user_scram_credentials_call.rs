//! Linear ownership of one accepted AnyBroker SCRAM credential-description call.

mod evidence;

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

pub(super) use evidence::DescribeUserScramCredentialsEvidence;

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted DescribeUserScramCredentials call must be terminally settled"]
pub(crate) struct DescribeUserScramCredentialsCall {
    call: Option<RoutedCall<DescribeUserScramCredentialsResponse>>,
    evidence: Option<DescribeUserScramCredentialsEvidence>,
}

impl DescribeUserScramCredentialsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: DescribeUserScramCredentialsPlan,
        request_limit: usize,
        result_limit: usize,
        deadline: Instant,
    ) -> Result<Self, DescribeUserScramCredentialsCallAdmissionFailure> {
        let evidence = DescribeUserScramCredentialsEvidence::new(plan, request_limit, result_limit);
        let request =
            describe_user_scram_credentials_request(request_ref(evidence.plan()), request_limit);
        let request = match request {
            Ok(request) => request,
            Err(_source) => {
                return Err(DescribeUserScramCredentialsCallAdmissionFailure::request(
                    evidence,
                ));
            }
        };
        let call = match driver.submit_describe_user_scram_credentials(request, deadline) {
            Ok(call) => call,
            Err(_source) => {
                return Err(DescribeUserScramCredentialsCallAdmissionFailure::driver(
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
    ) -> Option<Result<DescribeUserScramCredentialsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_describe_user_scram_credentials_terminal(
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
        plan: &DescribeUserScramCredentialsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.matches(plan, request_limit, result_limit))
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(
        self,
    ) -> Option<RecoveredDescribeUserScramCredentialsCall> {
        let Self { call, evidence } = self;
        call.zip(evidence).map(|(call, evidence)| {
            drop(call);
            RecoveredDescribeUserScramCredentialsCall::new(evidence)
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
enum DescribeUserScramCredentialsCallAdmissionSource {
    Request,
    Driver,
}

/// Definitely-unsent failure retaining the exact attempted SCRAM description.
#[must_use = "a rejected DescribeUserScramCredentials call must become operation input"]
pub(crate) struct DescribeUserScramCredentialsCallAdmissionFailure {
    source: DescribeUserScramCredentialsCallAdmissionSource,
    evidence: DescribeUserScramCredentialsEvidence,
}

impl DescribeUserScramCredentialsCallAdmissionFailure {
    const fn request(evidence: DescribeUserScramCredentialsEvidence) -> Self {
        Self {
            source: DescribeUserScramCredentialsCallAdmissionSource::Request,
            evidence,
        }
    }

    const fn driver(evidence: DescribeUserScramCredentialsEvidence) -> Self {
        Self {
            source: DescribeUserScramCredentialsCallAdmissionSource::Driver,
            evidence,
        }
    }

    pub(crate) fn into_evidence(self) -> (DescribeUserScramCredentialsPlan, usize, usize) {
        let Self { source, evidence } = self;
        let _ = source;
        evidence.into_parts()
    }
}
