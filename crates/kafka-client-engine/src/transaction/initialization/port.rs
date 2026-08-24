//! Public engine-boundary deadline capture, validation, and exact rejection.

use std::{sync::Arc, time::Duration};

use kafka_client_core::TransactionInitializationPlan;

use super::{
    TransactionInitializationAccepted, TransactionInitializationAdmissionError,
    TransactionInitializationAdmissionErrorKind, TransactionInitializationCapture,
    TransactionInitializationCaptureError, TransactionInitializationRequest,
    TransactionLifecycleControlPort, outcome::accepted_fault,
    shard::TransactionInitializationShardState,
};

#[derive(Clone)]
pub(crate) struct TransactionInitializationAdmissionPort {
    shared: Arc<TransactionInitializationShardState>,
}

impl TransactionInitializationAdmissionPort {
    pub(super) const fn new(shared: Arc<TransactionInitializationShardState>) -> Self {
        Self { shared }
    }

    pub(crate) fn capture(
        &self,
        operation_timeout: Duration,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Result<TransactionInitializationCapture, TransactionInitializationCaptureError> {
        let deadline = self
            .shared
            .clock()
            .capture_deadline_after(operation_timeout)
            .ok()
            .filter(|_capture| !operation_timeout.is_zero())
            .ok_or(TransactionInitializationCaptureError::InvalidOperationDeadline)?;
        Ok(TransactionInitializationCapture::new(
            Arc::clone(&self.shared),
            deadline,
            lifetime,
        ))
    }

    pub(crate) fn close_admission(&self) {
        self.shared.close();
        if let Ok(mut host) = self.shared.try_host() {
            host.close_admission();
        }
    }
}

pub(super) fn try_initialize_captured(
    shared: &Arc<TransactionInitializationShardState>,
    capture: crate::clock::DeadlineCapture,
    request: TransactionInitializationRequest,
    lifetime: Arc<dyn Send + Sync>,
) -> Result<TransactionInitializationAccepted, TransactionInitializationAdmissionError> {
    let plan = match validate(&request) {
        Ok(plan) => plan,
        Err(kind) => return Err(TransactionInitializationAdmissionError::new(kind, request)),
    };
    if shared.is_closed() {
        return Err(TransactionInitializationAdmissionError::new(
            TransactionInitializationAdmissionErrorKind::Closed,
            request,
        ));
    }
    let mut host = match shared.try_host() {
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
    let batch_record_capacity = host.execution_limits.batch_record_capacity();
    let mut admission = match host.try_admit(
        capture.now(),
        capture.operation_deadline(),
        request,
        plan,
        lifetime,
        TransactionLifecycleControlPort::new(Arc::clone(shared), batch_record_capacity),
    ) {
        Ok(admission) => admission,
        Err((kind, request)) => {
            return Err(TransactionInitializationAdmissionError::new(kind, request));
        }
    };
    drop(host);
    if shared.wake().request().is_err() {
        admission
            .fault
            .get_or_insert(super::TransactionInitializationHostError::Wake);
    }
    Ok(TransactionInitializationAccepted {
        observer: admission.observer,
        fault: admission.fault.map(accepted_fault),
    })
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
