//! Generated API-key 34 adaptation for one exact-broker replica-directory change.

mod model;
mod request;
mod response;
mod retention;
mod version;

pub(crate) use model::{
    AlterReplicaLogDirAssignmentRef, NormalizedAlterReplicaLogDirOutcome,
    NormalizedAlterReplicaLogDirsResponse,
};
#[cfg(test)]
pub(crate) use request::AlterReplicaLogDirsRequestFailure;
pub(crate) use request::alter_replica_log_dirs_request;
pub(crate) use response::{
    AlterReplicaLogDirsResponseFailure, normalize_alter_replica_log_dirs_response,
};
pub(crate) use version::{ALTER_REPLICA_LOG_DIRS_MAX_VERSION, ALTER_REPLICA_LOG_DIRS_MIN_VERSION};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod version_test;
