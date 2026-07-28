//! Declarative private bridge for replica log-directory alterations.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminAlterReplicaLogDirs;
pub(crate) use request::AlterReplicaLogDirsAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
