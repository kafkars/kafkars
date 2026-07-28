//! Declarative boundary for bounded transactional partition enrollment.

mod admission;
mod driver_port;
mod host;
#[cfg(test)]
mod host_failure_test;
#[cfg(test)]
mod host_port_test;
#[cfg(test)]
mod host_retry_test;
#[cfg(test)]
mod host_support_test;
#[cfg(test)]
mod host_test;
mod identity;
mod model;
#[cfg(test)]
mod model_test;
mod port;
mod settlement;

pub(crate) use host::TransactionPartitionEnrollmentOwner;
#[cfg(test)]
pub(crate) use model::TransactionPartitionEnrollmentFence;
pub(crate) use model::{
    TransactionPartitionEnrollmentAdmission, TransactionPartitionEnrollmentAdmissionFailure,
    TransactionPartitionEnrollmentEpochError, TransactionPartitionEnrollmentFailureKind,
    TransactionPartitionEnrollmentLimits, TransactionPartitionEnrollmentStartError,
    TransactionPartitionEnrollmentTerminal, TransactionPartitionEnrollmentTurn,
};
