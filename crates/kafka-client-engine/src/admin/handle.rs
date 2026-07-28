//! Runtime-neutral public handle retaining concrete admin admission ports.

use std::{fmt, sync::Arc, time::Duration};

use super::{
    CreateTopicsAdmissionError, CreateTopicsAdmissionErrorKind, CreateTopicsObserver,
    CreateTopicsRequest, DeleteTopicsAdmissionPort, shard::CreateTopicsAdmissionPort,
};
use crate::clock::MonotonicClock;

/// Closed set of concrete admin admission capabilities retained by one handle.
pub(crate) struct AdminAdmissionPorts {
    pub(crate) create_topics: CreateTopicsAdmissionPort,
    pub(crate) delete_topics: DeleteTopicsAdmissionPort,
    pub(crate) describe_cluster: super::DescribeClusterAdmissionPort,
    pub(crate) create_partitions: super::CreatePartitionsAdmissionPort,
    pub(crate) describe_topics: super::DescribeTopicsAdmissionPort,
    pub(crate) describe_configs: super::DescribeConfigsAdmissionPort,
    pub(crate) incremental_alter_configs: super::IncrementalAlterConfigsAdmissionPort,
    pub(crate) list_consumer_group_offsets: super::ListConsumerGroupOffsetsAdmissionPort,
    pub(crate) delete_consumer_group_offsets: super::DeleteConsumerGroupOffsetsAdmissionPort,
    pub(crate) alter_consumer_group_offsets: super::AlterConsumerGroupOffsetsAdmissionPort,
    pub(crate) list_offsets: super::AdminListOffsetsAdmissionPort,
    pub(crate) list_partition_reassignments: super::ListPartitionReassignmentsAdmissionPort,
}

/// Cheaply cloneable handle to the concrete admin shards.
#[derive(Clone)]
pub struct AdminHandle {
    pub(super) create_topics: CreateTopicsAdmissionPort,
    pub(super) delete_topics: DeleteTopicsAdmissionPort,
    pub(super) describe_cluster: super::DescribeClusterAdmissionPort,
    pub(super) create_partitions: super::CreatePartitionsAdmissionPort,
    pub(super) describe_topics: super::DescribeTopicsAdmissionPort,
    pub(super) describe_configs: super::DescribeConfigsAdmissionPort,
    pub(super) incremental_alter_configs: super::IncrementalAlterConfigsAdmissionPort,
    pub(super) list_consumer_group_offsets: super::ListConsumerGroupOffsetsAdmissionPort,
    pub(super) delete_consumer_group_offsets: super::DeleteConsumerGroupOffsetsAdmissionPort,
    pub(super) alter_consumer_group_offsets: super::AlterConsumerGroupOffsetsAdmissionPort,
    pub(super) list_offsets: super::AdminListOffsetsAdmissionPort,
    pub(super) list_partition_reassignments: super::ListPartitionReassignmentsAdmissionPort,
    pub(super) clock: Arc<MonotonicClock>,
    _lifetime: Arc<dyn Send + Sync>,
}

impl AdminHandle {
    pub(crate) fn new(
        ports: AdminAdmissionPorts,
        clock: Arc<MonotonicClock>,
        lifetime: Arc<dyn Send + Sync>,
    ) -> Self {
        Self {
            create_topics: ports.create_topics,
            delete_topics: ports.delete_topics,
            describe_cluster: ports.describe_cluster,
            create_partitions: ports.create_partitions,
            describe_topics: ports.describe_topics,
            describe_configs: ports.describe_configs,
            incremental_alter_configs: ports.incremental_alter_configs,
            list_consumer_group_offsets: ports.list_consumer_group_offsets,
            delete_consumer_group_offsets: ports.delete_consumer_group_offsets,
            alter_consumer_group_offsets: ports.alter_consumer_group_offsets,
            list_offsets: ports.list_offsets,
            list_partition_reassignments: ports.list_partition_reassignments,
            clock,
            _lifetime: lifetime,
        }
    }

    /// Attempts immediate bounded admission using one call-boundary deadline.
    pub fn try_create_topics(
        &self,
        request: CreateTopicsRequest,
        timeout: Duration,
    ) -> Result<CreateTopicsAccepted, CreateTopicsAdmissionError> {
        let capture = self
            .clock
            .capture_deadline_after(timeout)
            .map_err(|_error| {
                CreateTopicsAdmissionError::new(CreateTopicsAdmissionErrorKind::InvalidDeadline)
            })?;
        if timeout.is_zero() {
            return Err(CreateTopicsAdmissionError::new(
                CreateTopicsAdmissionErrorKind::InvalidDeadline,
            ));
        }
        let request = request.canonicalize();
        let retained_bytes = request.retained_charge().ok_or_else(|| {
            CreateTopicsAdmissionError::new(CreateTopicsAdmissionErrorKind::RetainedBytes)
        })?;
        let plan = request.into_plan().map_err(|_error| {
            CreateTopicsAdmissionError::new(CreateTopicsAdmissionErrorKind::InvalidRequest)
        })?;
        let admission = self
            .create_topics
            .try_admit(
                capture.now(),
                capture.operation_deadline(),
                plan,
                retained_bytes,
            )
            .map_err(CreateTopicsAdmissionError::new)?;
        Ok(CreateTopicsAccepted {
            observer: admission.observer,
            fault: admission.fault.map(accepted_fault_kind),
        })
    }
}

impl fmt::Debug for AdminHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminHandle")
            .finish_non_exhaustive()
    }
}

pub(super) const fn accepted_fault_kind(
    fault: super::CreateTopicsHostError,
) -> CreateTopicsAcceptedFaultKind {
    match fault {
        super::CreateTopicsHostError::Wake => CreateTopicsAcceptedFaultKind::Wake,
        _ => CreateTopicsAcceptedFaultKind::HostInvariant,
    }
}

/// Accepted post-commit degradation that cannot revoke operation ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateTopicsAcceptedFaultKind {
    /// The coalescing host wake failed after admission committed.
    Wake,
    /// An internal host invariant failed after terminal capacity was reserved.
    HostInvariant,
}

/// Accepted operation plus any post-commit wake degradation.
#[must_use = "accepted CreateTopics work must retain its observer"]
pub struct CreateTopicsAccepted {
    observer: CreateTopicsObserver,
    fault: Option<CreateTopicsAcceptedFaultKind>,
}

impl CreateTopicsAccepted {
    /// Returns any post-commit degradation without misclassifying ownership.
    pub const fn fault(&self) -> Option<CreateTopicsAcceptedFaultKind> {
        self.fault
    }

    /// Consumes the acceptance envelope into its named observer.
    pub fn into_observer(self) -> CreateTopicsObserver {
        self.observer
    }
}

impl fmt::Debug for CreateTopicsAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateTopicsAccepted")
            .field("observer", &self.observer)
            .field("fault", &self.fault)
            .finish()
    }
}
