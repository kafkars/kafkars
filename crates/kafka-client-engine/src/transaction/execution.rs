//! Declarative facade for one initialized transaction lifecycle.

mod host;
mod model;
mod send_admission;
#[cfg(test)]
mod send_admission_test;
mod topic_catalog;
mod turn;

pub(crate) use host::TransactionExecutionHost;
pub(crate) use model::{
    TransactionExecutionSendAdmissionError, TransactionExecutionSendAdmissionErrorKind,
};

#[cfg(test)]
mod host_test;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod topic_catalog_test;
