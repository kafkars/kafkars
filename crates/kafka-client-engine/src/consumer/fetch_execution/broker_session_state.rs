//! Infallible broker Fetch-session membership commits after reserved admission.

use kafka_client_core::{
    AssignedConsumerEffect, PositionFence, partitioning::TopicMetadataGeneration,
};

use crate::{driver::BrokerId, protocol::fetch::FetchSessionRequest};

use super::broker_session::{BrokerFetchSessions, BrokerSessionMember, BrokerSessionPlan};

pub(super) struct RetainedBrokerMember {
    pub(super) broker_id: BrokerId,
    pub(super) member: BrokerSessionMember,
    pub(super) forgotten: bool,
}

pub(super) struct BrokerSessionEntry {
    pub(super) broker_id: BrokerId,
    pub(super) metadata: FetchSessionRequest,
    pub(super) in_flight: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BrokerSessionError {
    Allocation,
    EntryCapacity,
    MemberCapacity,
    InFlight,
    MissingEntry,
    NotInFlight,
    PlanMismatch,
}

impl BrokerFetchSessions {
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn first_broker_id(&self) -> Option<BrokerId> {
        self.entries.first().map(|entry| entry.broker_id)
    }

    #[allow(
        clippy::type_complexity,
        reason = "the retained route tuple is immediately destructured by its sole production caller"
    )]
    pub(super) fn route_for_position(
        &self,
        position: PositionFence,
    ) -> Option<(
        BrokerId,
        [u8; 16],
        Option<i32>,
        Option<TopicMetadataGeneration>,
    )> {
        self.members
            .iter()
            .find(|retained| !retained.forgotten && retained.member.position() == position)
            .map(|retained| {
                (
                    retained.broker_id,
                    retained.member.topic_id(),
                    retained.member.leader_epoch(),
                    retained.member.metadata_generation(),
                )
            })
    }

    pub(super) fn newer_route_generation(
        &self,
        position: PositionFence,
        topic: &str,
    ) -> Option<TopicMetadataGeneration> {
        self.members
            .iter()
            .filter(|retained| retained.forgotten)
            .filter(|retained| retained.member.topic() == topic)
            .filter(|retained| retained.member.position().partition() == position.partition())
            .filter(|retained| {
                retained.member.position().assignment_epoch() != position.assignment_epoch()
            })
            .filter_map(|retained| retained.member.metadata_generation())
            .max()
    }

    pub(super) fn has_forgotten_ready(&self) -> bool {
        self.entries.iter().any(|entry| {
            !entry.in_flight
                && entry.metadata.is_incremental()
                && self
                    .members
                    .iter()
                    .any(|member| member.broker_id == entry.broker_id && member.forgotten)
        })
    }

    pub(super) fn discard_all(&mut self) {
        self.entries.clear();
        self.members.clear();
    }

    pub(super) fn observe_control(&mut self, effect: AssignedConsumerEffect) {
        for retained in &mut self.members {
            if control_covers(retained.member.position(), effect) {
                retained.forgotten = true;
            }
        }
    }

    pub(super) fn live_plan_index(
        &self,
        plan: &BrokerSessionPlan,
    ) -> Result<usize, BrokerSessionError> {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.broker_id == plan.broker_id())
        else {
            return Err(BrokerSessionError::MissingEntry);
        };
        if !self.entries[index].in_flight {
            return Err(BrokerSessionError::NotInFlight);
        }
        Ok(index)
    }

    pub(super) fn reset_index(&mut self, index: usize) {
        let broker_id = self.entries[index].broker_id;
        self.entries[index].metadata = FetchSessionRequest::INITIAL;
        self.entries[index].in_flight = false;
        self.members.retain(|member| member.broker_id != broker_id);
    }

    pub(super) fn remove_index(&mut self, index: usize) {
        let broker_id = self.entries.swap_remove(index).broker_id;
        self.members.retain(|member| member.broker_id != broker_id);
    }

    pub(super) fn apply_forgotten(&mut self, plan: &BrokerSessionPlan) {
        self.members.retain(|retained| {
            retained.broker_id != plan.broker_id()
                || !plan.forgotten().iter().any(|forgotten| {
                    forgotten.position().partition() == retained.member.position().partition()
                })
        });
    }

    pub(super) fn apply_active(&mut self, broker_id: BrokerId, active: Vec<BrokerSessionMember>) {
        for member in active {
            let partition = member.position().partition();
            for retained in &mut self.members {
                if retained.member.position().partition() == partition
                    && retained.broker_id != broker_id
                {
                    retained.forgotten = true;
                }
            }
            if let Some(retained) = self.members.iter_mut().find(|retained| {
                retained.broker_id == broker_id
                    && retained.member.position().partition() == partition
            }) {
                retained.member = member;
                retained.forgotten = false;
            } else {
                debug_assert!(self.members.len() < self.member_capacity);
                self.members.push(RetainedBrokerMember {
                    broker_id,
                    member,
                    forgotten: false,
                });
            }
        }
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> (usize, usize) {
        (self.entries.len(), self.members.len())
    }

    #[cfg(test)]
    pub(super) fn metadata(&self, broker_id: BrokerId) -> Option<FetchSessionRequest> {
        self.entries
            .iter()
            .find(|entry| entry.broker_id == broker_id)
            .map(|entry| entry.metadata)
    }
}

fn control_covers(position: PositionFence, effect: AssignedConsumerEffect) -> bool {
    match effect {
        AssignedConsumerEffect::Revoke {
            assignment_epoch,
            partition,
        } => position.assignment_epoch() == assignment_epoch && position.partition() == partition,
        AssignedConsumerEffect::Suspend { fence } => {
            position.assignment_epoch() == fence.assignment_epoch()
                && position.partition() == fence.partition()
                && position.position_epoch() < fence.position_epoch()
        }
        _ => false,
    }
}
