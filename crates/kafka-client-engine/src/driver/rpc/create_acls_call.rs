//! Linear ownership of one accepted AnyBroker `CreateAcls` call.

mod evidence;

use std::{mem::size_of, time::Instant};

use kafka_client_core::CreateAclsPlan;
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::CreateAclsResponse;

use crate::protocol::admin::create_acls::{CreateAclBindingRef, create_acls_request};

use super::{
    super::DriverOwner,
    create_acls_terminal::{
        CreateAclsRawTerminal, RecoveredCreateAclsCall, retain_create_acls_terminal,
    },
};

pub(super) use evidence::CreateAclsEvidence;

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted CreateAcls call must be terminally settled"]
pub(crate) struct CreateAclsCall {
    call: Option<RoutedCall<CreateAclsResponse>>,
    evidence: Option<CreateAclsEvidence>,
}

impl CreateAclsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: CreateAclsPlan,
        request_limit: usize,
        result_limit: usize,
        deadline: Instant,
    ) -> Result<Self, CreateAclsCallAdmissionFailure> {
        let evidence = CreateAclsEvidence::new(plan, request_limit, result_limit);
        let binding_count = evidence.plan().bindings().len();
        let mut binding_refs = Vec::new();
        if binding_refs.try_reserve_exact(binding_count).is_err() {
            return Err(CreateAclsCallAdmissionFailure::request(evidence));
        }
        let Some(binding_ref_bytes) = binding_refs
            .capacity()
            .checked_mul(size_of::<CreateAclBindingRef<'static>>())
        else {
            return Err(CreateAclsCallAdmissionFailure::request(evidence));
        };
        let Some(generated_limit) = request_limit.checked_sub(binding_ref_bytes) else {
            return Err(CreateAclsCallAdmissionFailure::request(evidence));
        };
        let bindings = evidence.plan().bindings();
        for binding in bindings {
            binding_refs.push(CreateAclBindingRef::new(
                binding.resource_type(),
                binding.resource_name(),
                binding.pattern_type(),
                binding.principal(),
                binding.host(),
                binding.operation(),
                binding.permission_type(),
            ));
        }
        let request = create_acls_request(&binding_refs, generated_limit);
        drop(binding_refs);
        let request = match request {
            Ok(request) => request,
            Err(_source) => return Err(CreateAclsCallAdmissionFailure::request(evidence)),
        };
        let call = match driver.submit_create_acls(request, deadline) {
            Ok(call) => call,
            Err(_source) => return Err(CreateAclsCallAdmissionFailure::driver(evidence)),
        };
        Ok(Self {
            call: Some(call),
            evidence: Some(evidence),
        })
    }

    /// Extracts a ready raw terminal without blocking or releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<CreateAclsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_create_acls_terminal(
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
        plan: &CreateAclsPlan,
        request_limit: usize,
        result_limit: usize,
    ) -> bool {
        self.evidence
            .as_ref()
            .is_some_and(|evidence| evidence.matches(plan, request_limit, result_limit))
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredCreateAclsCall> {
        let Self { call, evidence } = self;
        call.zip(evidence).map(|(call, evidence)| {
            drop(call);
            RecoveredCreateAclsCall::new(evidence)
        })
    }
}

/// Definitely-unsent failure from request construction or driver admission.
#[must_use = "a rejected CreateAcls call must become operation input"]
enum CreateAclsCallAdmissionFailureSource {
    Request,
    Driver,
}

/// Definitely-unsent failure retaining the exact attempted submission.
#[must_use = "a rejected CreateAcls call must become operation input"]
pub(crate) struct CreateAclsCallAdmissionFailure {
    source: CreateAclsCallAdmissionFailureSource,
    evidence: CreateAclsEvidence,
}

impl CreateAclsCallAdmissionFailure {
    const fn request(evidence: CreateAclsEvidence) -> Self {
        Self {
            source: CreateAclsCallAdmissionFailureSource::Request,
            evidence,
        }
    }

    const fn driver(evidence: CreateAclsEvidence) -> Self {
        Self {
            source: CreateAclsCallAdmissionFailureSource::Driver,
            evidence,
        }
    }

    pub(crate) fn into_submission_evidence(self) -> (CreateAclsPlan, usize, usize) {
        let Self { source, evidence } = self;
        let _ = source;
        evidence.into_parts()
    }
}
