//! Engine-owned scalar intent for one Admin `DescribeLogDirs` query.

use kafka_client_core::{
    AdminDescribeLogDirsPartition, AdminDescribeLogDirsPlan, AdminDescribeLogDirsPlanError,
};

/// One caller-ordered topic-partition selected on every queried broker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeLogDirTarget {
    topic: String,
    partition: i32,
}

impl DescribeLogDirTarget {
    /// Creates inert scalar intent validated at the operation boundary.
    pub const fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }

    /// Returns the selected topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the selected partition index.
    pub const fn partition(&self) -> i32 {
        self.partition
    }

    fn canonicalize(&mut self) {
        self.topic = core::mem::take(&mut self.topic)
            .into_boxed_str()
            .into_string();
    }

    fn into_core(self) -> AdminDescribeLogDirsPartition {
        AdminDescribeLogDirsPartition::new(self.topic, self.partition)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DescribeLogDirsRequestSelection {
    AllTopics,
    Selected(Vec<DescribeLogDirTarget>),
}

/// One caller-ordered selected-broker request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeLogDirsRequest {
    broker_ids: Vec<i32>,
    selection: DescribeLogDirsRequestSelection,
}

impl DescribeLogDirsRequest {
    /// Creates an all-topic request retained for source compatibility.
    pub const fn new(broker_ids: Vec<i32>) -> Self {
        Self::all(broker_ids)
    }

    /// Creates inert all-topic intent for validation at the operation boundary.
    pub const fn all(broker_ids: Vec<i32>) -> Self {
        Self {
            broker_ids,
            selection: DescribeLogDirsRequestSelection::AllTopics,
        }
    }

    /// Creates inert explicit partition intent for validation at operation boundary.
    pub const fn selected(broker_ids: Vec<i32>, partitions: Vec<DescribeLogDirTarget>) -> Self {
        Self {
            broker_ids,
            selection: DescribeLogDirsRequestSelection::Selected(partitions),
        }
    }

    pub(crate) fn canonicalize(mut self) -> Self {
        self.broker_ids.shrink_to_fit();
        if let DescribeLogDirsRequestSelection::Selected(partitions) = &mut self.selection {
            partitions
                .iter_mut()
                .for_each(DescribeLogDirTarget::canonicalize);
            partitions.shrink_to_fit();
        }
        self
    }

    pub(crate) fn into_plan(self) -> Result<AdminDescribeLogDirsPlan, DescribeLogDirsPlanFailure> {
        match self.selection {
            DescribeLogDirsRequestSelection::AllTopics => {
                AdminDescribeLogDirsPlan::new(self.broker_ids)
                    .map_err(DescribeLogDirsPlanFailure::Invalid)
            }
            DescribeLogDirsRequestSelection::Selected(partitions) => {
                let mut selected = Vec::new();
                selected
                    .try_reserve_exact(partitions.len())
                    .map_err(|_| DescribeLogDirsPlanFailure::RetainedBytes)?;
                selected.extend(partitions.into_iter().map(DescribeLogDirTarget::into_core));
                AdminDescribeLogDirsPlan::selected(self.broker_ids, selected)
                    .map_err(DescribeLogDirsPlanFailure::Invalid)
            }
        }
    }
}

/// Request conversion failure before atomic host admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DescribeLogDirsPlanFailure {
    /// Core rejected the selected broker or partition identities.
    Invalid(AdminDescribeLogDirsPlanError),
    /// Canonical core ownership could not fit an allocation.
    RetainedBytes,
}
