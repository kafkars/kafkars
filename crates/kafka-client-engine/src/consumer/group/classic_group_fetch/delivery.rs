//! Assignment-fenced transfer and exact reclamation of group Fetch deliveries.

use std::sync::Arc;

use kafka_client_core::{
    AssignmentEpoch, DeliveryOwnership, GroupAssignmentPartition, GroupId, GroupPositionFence,
    NextFetchOffset,
};

use crate::{
    consumer::{
        fetch_execution::{FetchExecutionError, FetchReclaimFailure},
        fetch_store::FetchDelivery,
    },
    protocol::fetch::FetchBatch,
};

use super::{
    super::session_catalog::GroupSessionCatalog,
    model::ClassicGroupFetchOwnerFault,
    owner::{ClassicGroupFetchOwner, FIRST_GROUP_FETCH_DELIVERIES},
};

/// One exact group Fetch byte lease before a processing lease or public batch exists.
///
/// The membership fence is retained beside the direct-consumer Fetch fence so
/// later registry and processing-lease layers do not have to reconstruct group
/// identity from mutable catalog state.
#[must_use = "a group Fetch delivery must be explicitly reclaimed by its exact owner"]
pub(in crate::consumer) struct ClassicGroupFetchDelivery {
    position_fence: GroupPositionFence,
    assignment_epoch: AssignmentEpoch,
    topic: Arc<str>,
    partition: i32,
    lease: FetchDelivery,
}

impl ClassicGroupFetchDelivery {
    pub(in crate::consumer::group) const fn group_id(&self) -> GroupId {
        self.position_fence.group_id()
    }

    pub(in crate::consumer) const fn position_fence(&self) -> GroupPositionFence {
        self.position_fence
    }

    pub(in crate::consumer::group) const fn assignment_epoch(&self) -> AssignmentEpoch {
        self.assignment_epoch
    }

    pub(in crate::consumer) fn topic(&self) -> &str {
        &self.topic
    }

    pub(in crate::consumer) fn topic_arc(&self) -> &Arc<str> {
        &self.topic
    }

    pub(in crate::consumer) const fn partition(&self) -> i32 {
        self.partition
    }

    pub(in crate::consumer) const fn next_offset(&self) -> NextFetchOffset {
        self.lease.next_offset()
    }

    pub(in crate::consumer) const fn partition_identity(
        &self,
    ) -> kafka_client_core::AssignedTopicPartition {
        self.lease.fence().position().partition()
    }

    pub(in crate::consumer) fn data_batches(&self) -> &[FetchBatch] {
        self.lease
            .outcome()
            .outcome()
            .data_batches()
            .unwrap_or_default()
    }

    fn new(
        position_fence: GroupPositionFence,
        assignment_epoch: AssignmentEpoch,
        topic: Arc<str>,
        partition: i32,
        lease: FetchDelivery,
    ) -> Self {
        Self {
            position_fence,
            assignment_epoch,
            topic,
            partition,
            lease,
        }
    }

    fn into_lease(self) -> FetchDelivery {
        self.lease
    }
}

/// Stable rejection before a group Fetch delivery lease transfers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer) enum ClassicGroupFetchDeliveryError {
    Faulted,
    EffectsPending,
    CatalogAssignmentMissing,
    CatalogAssignmentMismatch {
        expected: GroupPositionFence,
    },
    MachineAssignmentMismatch {
        expected: AssignmentEpoch,
        actual: Option<AssignmentEpoch>,
    },
    Retained,
}

/// A reclaim failure whose exact lease remains retained by the Fetch owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::consumer::group) enum ClassicGroupFetchReclaimError {
    Retained,
}

/// Reclaim failure retained independently of an earlier owner fault.
#[must_use = "the exact failed reclaim remains owned until shutdown recovery"]
pub(super) struct ClassicGroupFetchReclaimFault {
    error: FetchExecutionError,
    _delivery: FetchDelivery,
}

impl ClassicGroupFetchReclaimFault {
    fn new(failure: FetchReclaimFailure) -> Self {
        let (error, delivery) = failure.into_parts();
        Self {
            error,
            _delivery: delivery,
        }
    }

    pub(super) const fn error(&self) -> FetchExecutionError {
        self.error
    }
}

impl ClassicGroupFetchOwner {
    /// Transfers the oldest ready byte lease only while every group fence agrees.
    pub(in crate::consumer::group) fn take_delivery(
        &mut self,
        catalog: &GroupSessionCatalog,
    ) -> Result<Option<ClassicGroupFetchDelivery>, ClassicGroupFetchDeliveryError> {
        if self.is_faulted() {
            return Err(ClassicGroupFetchDeliveryError::Faulted);
        }
        if !self.effects.is_empty() {
            return Err(ClassicGroupFetchDeliveryError::EffectsPending);
        }
        let Some(activation) = self.activation.as_ref() else {
            return Ok(None);
        };
        let binding = activation.binding();
        let position_fence = binding.position_fence();
        let assignment_epoch = binding.assignment_epoch();
        let Some(assignment) = catalog.live_assignment() else {
            return Err(ClassicGroupFetchDeliveryError::CatalogAssignmentMissing);
        };
        if assignment.group_id() != position_fence.group_id()
            || assignment.member_id() != position_fence.member_id()
            || assignment.assignment_generation() != position_fence.assignment_generation()
        {
            return Err(ClassicGroupFetchDeliveryError::CatalogAssignmentMismatch {
                expected: position_fence,
            });
        }
        let machine_assignment = self.machine.assignment_epoch();
        if machine_assignment != Some(assignment_epoch) {
            return Err(ClassicGroupFetchDeliveryError::MachineAssignmentMismatch {
                expected: assignment_epoch,
                actual: machine_assignment,
            });
        }

        for _attempt in 0..FIRST_GROUP_FETCH_DELIVERIES {
            let delivery = match self.fetches.take_ready() {
                Ok(delivery) => delivery,
                Err(error) => {
                    self.fault = Some(ClassicGroupFetchOwnerFault::Fetch(error));
                    return Err(ClassicGroupFetchDeliveryError::Retained);
                }
            };
            let Some(delivery) = delivery else {
                return Ok(None);
            };
            match self.machine.delivery_ownership(delivery.fence()) {
                Ok(DeliveryOwnership::Active) => {}
                Ok(DeliveryOwnership::Superseded) => {
                    if self.reclaim_fetch_delivery(delivery).is_err() {
                        return Err(ClassicGroupFetchDeliveryError::Retained);
                    }
                    continue;
                }
                Err(error) => {
                    self.fault = Some(ClassicGroupFetchOwnerFault::Delivery {
                        error,
                        _delivery: delivery,
                    });
                    return Err(ClassicGroupFetchDeliveryError::Retained);
                }
            }
            let partition = delivery.fence().position().partition();
            if assignment
                .partitions()
                .binary_search(&GroupAssignmentPartition::new(
                    partition.topic_id(),
                    partition.partition(),
                ))
                .is_err()
            {
                self.fault = Some(ClassicGroupFetchOwnerFault::DeliveryPartition {
                    _delivery: delivery,
                });
                return Err(ClassicGroupFetchDeliveryError::Retained);
            }
            let topic = match catalog.topic_name(partition.topic_id()) {
                Ok(topic) => Arc::clone(topic),
                Err(error) => {
                    self.fault = Some(ClassicGroupFetchOwnerFault::DeliveryCatalog {
                        error,
                        _delivery: delivery,
                    });
                    return Err(ClassicGroupFetchDeliveryError::Retained);
                }
            };
            return Ok(Some(ClassicGroupFetchDelivery::new(
                position_fence,
                assignment_epoch,
                topic,
                partition.partition().get().cast_signed(),
                delivery,
            )));
        }
        Ok(None)
    }

    /// Returns one externally held group Fetch lease to its exact byte owner.
    ///
    /// Assignment loss does not invalidate reclamation. If the underlying store
    /// rejects the return, the exact delivery remains retained inside this owner
    /// for post-driver shutdown recovery.
    pub(in crate::consumer::group) fn reclaim_delivery(
        &mut self,
        delivery: ClassicGroupFetchDelivery,
    ) -> Result<(), ClassicGroupFetchReclaimError> {
        self.reclaim_fetch_delivery(delivery.into_lease())
    }

    fn reclaim_fetch_delivery(
        &mut self,
        delivery: FetchDelivery,
    ) -> Result<(), ClassicGroupFetchReclaimError> {
        self.fetches.reclaim(delivery).map_err(|failure| {
            let fault = ClassicGroupFetchReclaimFault::new(failure);
            if self.reclaim_faults.len() < FIRST_GROUP_FETCH_DELIVERIES {
                self.reclaim_faults.push(fault);
            } else {
                self.reclaim_overflow = Some(fault);
            }
            ClassicGroupFetchReclaimError::Retained
        })
    }
}
