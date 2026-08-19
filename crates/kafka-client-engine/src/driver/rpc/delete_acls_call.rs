//! Linear ownership of one accepted `AnyBroker` `DeleteAcls` call.

mod evidence;

use std::{mem::size_of, time::Instant};

use kafka_client_core::{DeleteAclsFilter, DeleteAclsPlan};
use kafka_driver::{CompletionError, RoutedCall};
use kafka_wire::DeleteAclsResponse;

use crate::protocol::admin::delete_acls::{DeleteAclsFilterRef, delete_acls_request};

use super::{
    super::DriverOwner,
    delete_acls_terminal::{
        DeleteAclsRawTerminal, RecoveredDeleteAclsCall, retain_delete_acls_terminal,
    },
};

pub(super) use evidence::DeleteAclsEvidence;

/// One accepted driver call retained beside its concrete admin owner.
#[must_use = "an accepted DeleteAcls call must be terminally settled"]
pub(crate) struct DeleteAclsCall {
    call: Option<RoutedCall<DeleteAclsResponse>>,
    evidence: Option<DeleteAclsEvidence>,
}

impl DeleteAclsCall {
    pub(crate) fn submit(
        driver: &DriverOwner,
        plan: DeleteAclsPlan,
        request_limit: usize,
        nested_count_capacity: usize,
        result_capacity: usize,
        outcome_capacity: usize,
        deadline: Instant,
    ) -> Result<Self, DeleteAclsCallAdmissionFailure> {
        let evidence = DeleteAclsEvidence::new(
            plan,
            request_limit,
            nested_count_capacity,
            result_capacity,
            outcome_capacity,
        );
        let (filter_refs, nested_request_limit) =
            match prepare_delete_acls_filter_refs(evidence.plan().filters(), request_limit) {
                Ok(prepared) => prepared,
                Err(source) => return Err(DeleteAclsCallAdmissionFailure::new(source, evidence)),
            };
        let request = delete_acls_request(&filter_refs, nested_request_limit);
        drop(filter_refs);
        let request = match request {
            Ok(request) => request,
            Err(_source) => {
                return Err(DeleteAclsCallAdmissionFailure::new(
                    DeleteAclsCallAdmissionSource::Request,
                    evidence,
                ));
            }
        };
        let call = match driver.submit_delete_acls(request, deadline) {
            Ok(call) => call,
            Err(_source) => {
                return Err(DeleteAclsCallAdmissionFailure::new(
                    DeleteAclsCallAdmissionSource::Driver,
                    evidence,
                ));
            }
        };
        Ok(Self {
            call: Some(call),
            evidence: Some(evidence),
        })
    }

    /// Extracts a ready raw terminal without blocking or releasing route evidence.
    pub(crate) fn try_terminal(
        &mut self,
    ) -> Option<Result<DeleteAclsRawTerminal, CompletionError>> {
        let result = self.call.as_mut()?.try_result()?;
        match result {
            Ok(outcome) => {
                let evidence = self.evidence.take()?;
                drop(self.call.take());
                let (result, selected_version, route_token) = outcome.into_parts();
                Some(Ok(retain_delete_acls_terminal(
                    selected_version,
                    result,
                    route_token,
                    evidence,
                )))
            }
            Err(source) => Some(Err(source)),
        }
    }

    pub(crate) fn matches(
        &self,
        plan: &DeleteAclsPlan,
        request_limit: usize,
        nested_count_capacity: usize,
        result_capacity: usize,
        outcome_capacity: usize,
    ) -> bool {
        self.evidence.as_ref().is_some_and(|evidence| {
            evidence.matches(
                plan,
                request_limit,
                nested_count_capacity,
                result_capacity,
                outcome_capacity,
            )
        })
    }

    /// Seals an unresolved call only after the unique driver is gone.
    pub(crate) fn recover_after_driver_shutdown(self) -> Option<RecoveredDeleteAclsCall> {
        let Self { call, evidence } = self;
        match (call, evidence) {
            (Some(call), Some(evidence)) => {
                drop(call);
                Some(RecoveredDeleteAclsCall::new(evidence))
            }
            _ => None,
        }
    }
}

pub(super) fn prepare_delete_acls_filter_refs(
    filters: &[DeleteAclsFilter],
    retained_limit: usize,
) -> Result<(Vec<DeleteAclsFilterRef<'_>>, usize), DeleteAclsCallAdmissionSource> {
    let minimum_bytes = filter_ref_bytes(filters.len())?;
    retained_limit
        .checked_sub(minimum_bytes)
        .ok_or(DeleteAclsCallAdmissionSource::Request)?;
    let mut filter_refs = Vec::new();
    filter_refs
        .try_reserve_exact(filters.len())
        .map_err(|_| DeleteAclsCallAdmissionSource::Request)?;
    for filter in filters {
        filter_refs.push(DeleteAclsFilterRef::new(
            filter.resource_type(),
            filter.resource_name(),
            filter.pattern_type(),
            filter.principal(),
            filter.host(),
            filter.operation(),
            filter.permission_type(),
        ));
    }
    let actual_bytes = filter_ref_bytes(filter_refs.capacity())?;
    let request_limit = retained_limit
        .checked_sub(actual_bytes)
        .ok_or(DeleteAclsCallAdmissionSource::Request)?;
    Ok((filter_refs, request_limit))
}

fn filter_ref_bytes(count: usize) -> Result<usize, DeleteAclsCallAdmissionSource> {
    count
        .checked_mul(size_of::<DeleteAclsFilterRef<'static>>())
        .ok_or(DeleteAclsCallAdmissionSource::Request)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeleteAclsCallAdmissionSource {
    Request,
    Driver,
}

/// Definitely-unsent failure retaining exact attempted ACL deletion.
#[derive(Debug)]
#[must_use = "a rejected DeleteAcls call must become operation input"]
pub(crate) struct DeleteAclsCallAdmissionFailure {
    source: DeleteAclsCallAdmissionSource,
    evidence: DeleteAclsEvidence,
}

impl DeleteAclsCallAdmissionFailure {
    const fn new(source: DeleteAclsCallAdmissionSource, evidence: DeleteAclsEvidence) -> Self {
        Self { source, evidence }
    }

    pub(crate) fn into_evidence(self) -> (DeleteAclsPlan, usize, usize, usize, usize) {
        let Self { source, evidence } = self;
        let _ = source;
        evidence.into_parts()
    }
}
