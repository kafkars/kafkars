//! Engine-owned canonical request intent for consumer-group offset alteration.

use core::mem::size_of;
use std::time::Duration;

use kafka_client_core::{
    AlterConsumerGroupOffsetTarget as CoreTarget, AlterConsumerGroupOffsetsPlan,
    AlterConsumerGroupOffsetsPlanError,
};

/// One caller-ordered topic-partition whose committed offset must be altered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterConsumerGroupOffsetTarget {
    topic: String,
    partition: i32,
    next_offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

impl AlterConsumerGroupOffsetTarget {
    /// Creates one raw target for validation at admission.
    pub const fn new(
        topic: String,
        partition: i32,
        next_offset: i64,
        leader_epoch: Option<i32>,
        metadata: Option<String>,
    ) -> Self {
        Self {
            topic,
            partition,
            next_offset,
            leader_epoch,
            metadata,
        }
    }

    fn canonicalize(mut self) -> Self {
        self.topic = canonical_string(self.topic);
        self.metadata = self.metadata.map(canonical_string);
        self
    }

    fn into_core(self) -> CoreTarget {
        CoreTarget::new(
            self.topic,
            self.partition,
            self.next_offset,
            self.leader_epoch,
            self.metadata,
        )
    }
}

/// One explicit group and nonempty caller-ordered alteration batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlterConsumerGroupOffsetsRequest {
    group_id: String,
    targets: Vec<AlterConsumerGroupOffsetTarget>,
    retention_time: Option<Duration>,
}

impl AlterConsumerGroupOffsetsRequest {
    /// Creates one inert request for validation at the public call boundary.
    pub const fn new(group_id: String, targets: Vec<AlterConsumerGroupOffsetTarget>) -> Self {
        Self {
            group_id,
            targets,
            retention_time: None,
        }
    }

    /// Selects an explicit retention duration without starting or validating work.
    pub const fn with_retention_time(mut self, retention_time: Duration) -> Self {
        self.retention_time = Some(retention_time);
        self
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.group_id = canonical_string(self.group_id);
        self.targets = canonical_vec(
            self.targets
                .into_iter()
                .map(AlterConsumerGroupOffsetTarget::canonicalize)
                .collect(),
        );
        self
    }

    pub(crate) fn preparation_charge(&self) -> Option<usize> {
        self.targets.iter().try_fold(
            size_of::<Self>()
                .checked_add(self.group_id.len())?
                .checked_add(
                    self.targets
                        .len()
                        .checked_mul(size_of::<AlterConsumerGroupOffsetTarget>())?,
                )?,
            |bytes, target| {
                bytes.checked_add(target.topic.len()).and_then(|bytes| {
                    bytes.checked_add(target.metadata.as_ref().map_or(0, String::len))
                })
            },
        )
    }

    pub(crate) fn into_plan(
        self,
    ) -> Result<AlterConsumerGroupOffsetsPlan, AlterConsumerGroupOffsetsPlanError> {
        let retention_time_ms = self
            .retention_time
            .map(|retention_time| {
                i64::try_from(retention_time.as_millis())
                    .map_err(|_| AlterConsumerGroupOffsetsPlanError::RetentionTimeTooLarge)
            })
            .transpose()?;
        let plan = AlterConsumerGroupOffsetsPlan::new(
            self.group_id,
            self.targets
                .into_iter()
                .map(AlterConsumerGroupOffsetTarget::into_core)
                .collect(),
        )?;
        match retention_time_ms {
            Some(retention_time_ms) => plan.with_retention_time_ms(retention_time_ms),
            None => Ok(plan),
        }
    }

    #[cfg(test)]
    pub(super) fn storage_is_canonical(&self) -> bool {
        self.group_id.capacity() == self.group_id.len()
            && self.targets.capacity() == self.targets.len()
            && self.targets.iter().all(|target| {
                target.topic.capacity() == target.topic.len()
                    && target
                        .metadata
                        .as_ref()
                        .is_none_or(|metadata| metadata.capacity() == metadata.len())
            })
    }
}

fn canonical_string(value: String) -> String {
    value.into_boxed_str().into_string()
}

fn canonical_vec<T>(value: Vec<T>) -> Vec<T> {
    value.into_boxed_slice().into_vec()
}
