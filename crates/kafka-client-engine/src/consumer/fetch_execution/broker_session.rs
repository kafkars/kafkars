//! Bounded broker-owned Fetch-session membership, epochs, and control transitions.

use std::sync::Arc;

use kafka_client_core::PositionFence;
use kafka_driver::BrokerId;

use crate::protocol::fetch::{FetchSessionRequest, FetchSessionUpdate};

use super::broker_session_state::{BrokerSessionEntry, BrokerSessionError, RetainedBrokerMember};

/// Stable topic-partition identity retained while one broker caches it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrokerSessionMember {
    position: PositionFence,
    topic: Arc<str>,
}

impl BrokerSessionMember {
    pub(super) fn new(position: PositionFence, topic: Arc<str>) -> Self {
        Self { position, topic }
    }

    pub(super) const fn position(&self) -> PositionFence {
        self.position
    }

    pub(super) fn topic(&self) -> &str {
        &self.topic
    }
}

/// Linear request-side snapshot whose completion advances exactly one broker epoch.
#[must_use = "a begun broker Fetch session must be completed, aborted, or closed"]
pub(super) struct BrokerSessionPlan {
    pub(super) broker_id: BrokerId,
    pub(super) session: FetchSessionRequest,
    pub(super) active: Vec<BrokerSessionMember>,
    pub(super) forgotten: Vec<BrokerSessionMember>,
    pub(super) close: bool,
}

impl BrokerSessionPlan {
    pub(super) const fn broker_id(&self) -> BrokerId {
        self.broker_id
    }

    pub(super) const fn session(&self) -> FetchSessionRequest {
        self.session
    }

    pub(super) fn active(&self) -> &[BrokerSessionMember] {
        &self.active
    }

    pub(super) fn forgotten(&self) -> &[BrokerSessionMember] {
        &self.forgotten
    }

    pub(super) const fn is_close(&self) -> bool {
        self.close
    }
}

/// Sole bounded owner of every broker session used by one consumer lane.
pub(super) struct BrokerFetchSessions {
    pub(super) entry_capacity: usize,
    pub(super) member_capacity: usize,
    pub(super) entries: Vec<BrokerSessionEntry>,
    pub(super) members: Vec<RetainedBrokerMember>,
}

impl BrokerFetchSessions {
    pub(super) fn try_new(
        entry_capacity: usize,
        member_capacity: usize,
    ) -> Result<Self, BrokerSessionError> {
        let mut entries = Vec::new();
        let mut members = Vec::new();
        entries
            .try_reserve_exact(entry_capacity)
            .map_err(|_error| BrokerSessionError::Allocation)?;
        members
            .try_reserve_exact(member_capacity)
            .map_err(|_error| BrokerSessionError::Allocation)?;
        Ok(Self {
            entry_capacity,
            member_capacity,
            entries,
            members,
        })
    }

    #[allow(
        clippy::result_large_err,
        reason = "failed begin returns exact active ownership"
    )]
    pub(super) fn try_begin(
        &mut self,
        broker_id: BrokerId,
        active: Vec<BrokerSessionMember>,
    ) -> Result<BrokerSessionPlan, (BrokerSessionError, Vec<BrokerSessionMember>)> {
        let entry = self
            .entries
            .iter()
            .position(|entry| entry.broker_id == broker_id);
        if entry.is_none() && self.entries.len() >= self.entry_capacity {
            return Err((BrokerSessionError::EntryCapacity, active));
        }
        if entry.is_some_and(|index| self.entries[index].in_flight) {
            return Err((BrokerSessionError::InFlight, active));
        }
        let additions = active
            .iter()
            .filter(|active| {
                !self.members.iter().any(|retained| {
                    retained.broker_id == broker_id
                        && retained.member.position().partition() == active.position().partition()
                })
            })
            .count();
        if self.members.len().saturating_add(additions) > self.member_capacity {
            return Err((BrokerSessionError::MemberCapacity, active));
        }
        let forgotten_count = self
            .members
            .iter()
            .filter(|member| member.broker_id == broker_id && member.forgotten)
            .count();
        let mut forgotten = Vec::new();
        if forgotten.try_reserve_exact(forgotten_count).is_err() {
            return Err((BrokerSessionError::Allocation, active));
        }
        forgotten.extend(
            self.members
                .iter()
                .filter(|member| member.broker_id == broker_id && member.forgotten)
                .map(|member| member.member.clone()),
        );
        let index = entry.unwrap_or_else(|| {
            self.entries.push(BrokerSessionEntry {
                broker_id,
                metadata: FetchSessionRequest::INITIAL,
                in_flight: false,
            });
            self.entries.len().saturating_sub(1)
        });
        self.entries[index].in_flight = true;
        Ok(BrokerSessionPlan {
            broker_id,
            session: self.entries[index].metadata,
            active,
            forgotten,
            close: false,
        })
    }

    pub(super) fn complete(
        &mut self,
        plan: BrokerSessionPlan,
        update: FetchSessionUpdate,
    ) -> Result<(), BrokerSessionError> {
        let index = self.live_plan_index(&plan)?;
        if plan.is_close() || self.entries[index].metadata != plan.session() {
            return Err(BrokerSessionError::PlanMismatch);
        }
        match update {
            FetchSessionUpdate::Reset => self.reset_index(index),
            FetchSessionUpdate::Continue(metadata) => {
                self.entries[index].metadata = metadata;
                self.entries[index].in_flight = false;
                self.apply_forgotten(&plan);
                self.apply_active(plan.broker_id, plan.active);
            }
        }
        Ok(())
    }

    pub(super) fn abort(
        &mut self,
        plan: BrokerSessionPlan,
        reset: bool,
    ) -> Result<(), BrokerSessionError> {
        let index = self.live_plan_index(&plan)?;
        if reset {
            self.reset_index(index);
        } else {
            self.entries[index].in_flight = false;
        }
        Ok(())
    }

    pub(super) fn try_begin_close(
        &mut self,
        broker_id: BrokerId,
    ) -> Result<Option<BrokerSessionPlan>, BrokerSessionError> {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.broker_id == broker_id)
        else {
            return Ok(None);
        };
        if self.entries[index].in_flight {
            return Err(BrokerSessionError::InFlight);
        }
        let Some(close) = self.entries[index].metadata.close() else {
            self.remove_index(index);
            return Ok(None);
        };
        self.entries[index].in_flight = true;
        Ok(Some(BrokerSessionPlan {
            broker_id,
            session: close,
            active: Vec::new(),
            forgotten: Vec::new(),
            close: true,
        }))
    }

    pub(super) fn complete_close(
        &mut self,
        plan: BrokerSessionPlan,
    ) -> Result<(), BrokerSessionError> {
        let index = self.live_plan_index(&plan)?;
        if !plan.is_close() || self.entries[index].metadata.close() != Some(plan.session()) {
            return Err(BrokerSessionError::PlanMismatch);
        }
        self.remove_index(index);
        Ok(())
    }
}
