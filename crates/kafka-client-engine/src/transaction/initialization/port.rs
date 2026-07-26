//! Public engine-boundary deadline capture, validation, and exact rejection.

use std::{sync::Arc, time::Duration};

use kafka_client_core::TransactionInitializationPlan;

use super::{
    TransactionInitializationAccepted, TransactionInitializationAdmissionError,
    TransactionInitializationAdmissionErrorKind, TransactionInitializationRequest,
    outcome::accepted_fault, shard::TransactionInitializationShardState,
};

#[derive(Clone)]
pub(crate) struct TransactionInitializationAdmissionPort {
    shared: Arc<TransactionInitializationShardState>,
}

impl TransactionInitializationAdmissionPort {
    pub(super) const fn new(shared: Arc<TransactionInitializationShardState>) -> Self {
        Self { shared }
    }

    pub(crate) fn try_initialize(
        &self,
        request: TransactionInitializationRequest,
        operation_timeout: Duration,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Result<TransactionInitializationAccepted, TransactionInitializationAdmissionError> {
        let capture = match self
            .shared
            .clock()
            .capture_deadline_after(operation_timeout)
        {
            Ok(capture) if !operation_timeout.is_zero() => capture,
            Ok(_) | Err(_) => {
                return Err(TransactionInitializationAdmissionError::new(
                    TransactionInitializationAdmissionErrorKind::InvalidOperationDeadline,
                    request,
                ));
            }
        };
        let plan = match validate(&request) {
            Ok(plan) => plan,
            Err(kind) => return Err(TransactionInitializationAdmissionError::new(kind, request)),
        };
        if self.shared.is_closed() {
            return Err(TransactionInitializationAdmissionError::new(
                TransactionInitializationAdmissionErrorKind::Closed,
                request,
            ));
        }
        let mut host = match self.shared.try_host() {
            Ok(host) => host,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(TransactionInitializationAdmissionError::new(
                    TransactionInitializationAdmissionErrorKind::Contended,
                    request,
                ));
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(TransactionInitializationAdmissionError::new(
                    TransactionInitializationAdmissionErrorKind::HostUnavailable,
                    request,
                ));
            }
        };
        let mut admission = match host.try_admit(
            capture.now(),
            capture.operation_deadline(),
            request,
            plan,
            lifetime,
        ) {
            Ok(admission) => admission,
            Err((kind, request)) => {
                return Err(TransactionInitializationAdmissionError::new(kind, request));
            }
        };
        drop(host);
        if self.shared.wake().request().is_err() {
            admission
                .fault
                .get_or_insert(super::TransactionInitializationHostError::Wake);
        }
        Ok(TransactionInitializationAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault),
        })
    }

    pub(crate) fn close_admission(&self) {
        self.shared.close();
        if let Ok(mut host) = self.shared.try_host() {
            host.close_admission();
        }
    }
}

pub(super) fn validate(
    request: &TransactionInitializationRequest,
) -> Result<TransactionInitializationPlan, TransactionInitializationAdmissionErrorKind> {
    let transactional_id = request.transactional_id();
    if transactional_id.is_empty() || transactional_id.len() > i16::MAX as usize {
        return Err(TransactionInitializationAdmissionErrorKind::InvalidRequest);
    }
    if request.transactional_id_capacity() > i16::MAX as usize {
        return Err(TransactionInitializationAdmissionErrorKind::RetainedBytes);
    }
    TransactionInitializationPlan::new(request.transaction_timeout_ms())
        .map_err(|_error| TransactionInitializationAdmissionErrorKind::InvalidRequest)
}
