//! Declarative private bridge for selected-replica log-directory descriptions.

mod engine;
mod operation;
mod request;
mod result;

pub(crate) use operation::AdminDescribeReplicaLogDirs;
pub(crate) use request::DescribeReplicaLogDirsAdminRequest;

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod result_test;
