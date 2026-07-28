//! Engine-owned scalar intent for one Admin `ListOffsets` query.

use kafka_client_core::{
    AdminListOffsetSpec, AdminListOffsetTarget, AdminListOffsetsPlan, AdminListOffsetsPlanError,
};

use crate::config::ConsumerReadIsolation;

/// Stable engine request specification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminListOffsetsRequestSpec {
    /// Select the earliest available offset.
    Earliest,
    /// Select the latest available offset.
    Latest,
    /// Select the record offset carrying the greatest timestamp.
    MaxTimestamp,
    /// Select the local-log start offset.
    EarliestLocal,
    /// Select the greatest offset already retained in tiered storage.
    LatestTiered,
    /// Select the earliest offset not yet uploaded to tiered storage.
    EarliestPendingUpload,
    /// Select the earliest offset at or after this nonnegative timestamp.
    Timestamp(i64),
}

/// One engine-owned topic-partition request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListOffsetsRequestTarget {
    topic: String,
    partition: i32,
    spec: AdminListOffsetsRequestSpec,
    current_leader_epoch: Option<i32>,
}

impl AdminListOffsetsRequestTarget {
    /// Creates one inert target for validation at admission.
    pub const fn new(topic: String, partition: i32, spec: AdminListOffsetsRequestSpec) -> Self {
        Self {
            topic,
            partition,
            spec,
            current_leader_epoch: None,
        }
    }

    /// Replaces the optional leader epoch used to fence stale partition leaders.
    pub const fn current_leader_epoch(mut self, current_leader_epoch: i32) -> Self {
        self.current_leader_epoch = Some(current_leader_epoch);
        self
    }

    fn canonicalize(mut self) -> Self {
        self.topic = self.topic.into_boxed_str().into_string();
        self
    }

    fn into_core(self) -> AdminListOffsetTarget {
        let spec = match self.spec {
            AdminListOffsetsRequestSpec::Earliest => AdminListOffsetSpec::Earliest,
            AdminListOffsetsRequestSpec::Latest => AdminListOffsetSpec::Latest,
            AdminListOffsetsRequestSpec::MaxTimestamp => AdminListOffsetSpec::MaxTimestamp,
            AdminListOffsetsRequestSpec::EarliestLocal => AdminListOffsetSpec::EarliestLocal,
            AdminListOffsetsRequestSpec::LatestTiered => AdminListOffsetSpec::LatestTiered,
            AdminListOffsetsRequestSpec::EarliestPendingUpload => {
                AdminListOffsetSpec::EarliestPendingUpload
            }
            AdminListOffsetsRequestSpec::Timestamp(timestamp) => {
                AdminListOffsetSpec::Timestamp(timestamp)
            }
        };
        AdminListOffsetTarget::new(self.topic, self.partition, spec)
            .with_current_leader_epoch(self.current_leader_epoch)
    }
}

/// One caller-ordered bounded request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdminListOffsetsRequest {
    targets: Vec<AdminListOffsetsRequestTarget>,
    read_isolation: ConsumerReadIsolation,
}

impl AdminListOffsetsRequest {
    /// Creates inert request intent for validation at the operation boundary.
    pub const fn new(targets: Vec<AdminListOffsetsRequestTarget>) -> Self {
        Self {
            targets,
            read_isolation: ConsumerReadIsolation::ReadUncommitted,
        }
    }

    /// Replaces the immutable visibility policy applied to every target.
    pub const fn with_read_isolation(mut self, read_isolation: ConsumerReadIsolation) -> Self {
        self.read_isolation = read_isolation;
        self
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.targets = self
            .targets
            .into_iter()
            .map(AdminListOffsetsRequestTarget::canonicalize)
            .collect();
        self.targets.shrink_to_fit();
        self
    }

    pub(crate) fn into_plan(self) -> Result<AdminListOffsetsPlan, AdminListOffsetsPlanError> {
        AdminListOffsetsPlan::with_read_isolation(
            self.targets
                .into_iter()
                .map(AdminListOffsetsRequestTarget::into_core)
                .collect(),
            self.read_isolation.core(),
        )
    }
}
