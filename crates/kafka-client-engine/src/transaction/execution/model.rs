//! Exact unresolved input and stable owner-fenced admission categories.

use crate::transaction::send::{TransactionSendAdmissionFailureKind, TransactionSendInput};

use super::topic_catalog::TransactionTopicCatalogError;

/// Stable reason an unresolved send did not cross deterministic acceptance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionExecutionSendAdmissionErrorKind {
    StaleOwner,
    BatchRecordCapacity { actual: usize, limit: usize },
    RetainedRecordBytes { actual: usize, limit: usize },
    RetainedTopicCapacity { actual: usize, limit: usize },
    RetainedTopicBytes { actual: usize, limit: usize },
    RetainedTopicBytesOverflow,
    TopicIdentityExhausted,
    Allocation,
    Send(TransactionSendAdmissionFailureKind),
}

/// Exact unresolved input after execution-host send rejection.
#[must_use = "rejected transactional send admission returns its exact unresolved input"]
#[derive(Debug)]
pub(crate) struct TransactionExecutionSendAdmissionError {
    kind: TransactionExecutionSendAdmissionErrorKind,
    input: TransactionSendInput,
}

impl TransactionExecutionSendAdmissionError {
    pub(in crate::transaction) const fn new(
        kind: TransactionExecutionSendAdmissionErrorKind,
        input: TransactionSendInput,
    ) -> Self {
        Self { kind, input }
    }

    pub(crate) const fn kind(&self) -> TransactionExecutionSendAdmissionErrorKind {
        self.kind
    }

    pub(crate) fn into_input(self) -> TransactionSendInput {
        self.input
    }
}

impl From<TransactionTopicCatalogError> for TransactionExecutionSendAdmissionErrorKind {
    fn from(error: TransactionTopicCatalogError) -> Self {
        match error {
            TransactionTopicCatalogError::RetainedTopicCapacity { actual, limit } => {
                Self::RetainedTopicCapacity { actual, limit }
            }
            TransactionTopicCatalogError::RetainedTopicBytes { actual, limit } => {
                Self::RetainedTopicBytes { actual, limit }
            }
            TransactionTopicCatalogError::RetainedTopicBytesOverflow => {
                Self::RetainedTopicBytesOverflow
            }
            TransactionTopicCatalogError::TopicIdentityExhausted => Self::TopicIdentityExhausted,
            TransactionTopicCatalogError::Allocation => Self::Allocation,
        }
    }
}
