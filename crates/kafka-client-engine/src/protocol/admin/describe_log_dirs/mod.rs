//! Generated API-key 35 adaptation for one broker-scoped log-directory query.

mod model;
mod request;
mod response;
mod retention;
mod version;

pub(crate) use model::{
    DescribeLogDirsSelectionRef, DescribeLogDirsTopicSelectionRef, NormalizedDescribeLogDir,
    NormalizedDescribeLogDirsPartition, NormalizedDescribeLogDirsResponse,
    NormalizedDescribeLogDirsTopic,
};
pub(crate) use request::describe_log_dirs_request;
pub(crate) use response::{DescribeLogDirsResponseFailure, normalize_describe_log_dirs_response};

#[cfg(test)]
pub(crate) use request::DescribeLogDirsRequestFailure;
#[cfg(test)]
pub(crate) use version::{DESCRIBE_LOG_DIRS_MAX_VERSION, DESCRIBE_LOG_DIRS_MIN_VERSION};

#[cfg(test)]
mod request_test;
#[cfg(test)]
mod response_test;
#[cfg(test)]
mod retention_test;
#[cfg(test)]
mod version_test;
