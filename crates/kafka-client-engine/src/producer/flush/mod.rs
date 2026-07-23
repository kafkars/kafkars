//! Producer flush admission and completion-generation ownership.

mod admission;
mod binding;

pub(crate) use admission::{AdmittedFlush, FlushAdmissionFailure, FlushRejectionReason};
pub(crate) use binding::{FlushBindingError, FlushBindings};

#[cfg(test)]
mod admission_test;
#[cfg(test)]
mod binding_test;
#[cfg(test)]
mod execution_stop_test;
