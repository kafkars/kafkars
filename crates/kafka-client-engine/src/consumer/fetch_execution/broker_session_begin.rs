//! Capacity-preflighted planning for one broker Fetch-session epoch.

use crate::{driver::BrokerId, protocol::fetch::FetchSessionRequest};

use super::{
    broker_session::{BrokerFetchSessions, BrokerSessionMember, BrokerSessionPlan},
    broker_session_state::{BrokerSessionEntry, BrokerSessionError},
};

impl BrokerFetchSessions {
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
        let forgotten_count = self
            .members
            .iter()
            .filter(|retained| {
                retained.broker_id == broker_id
                    && retained.forgotten
                    && !active.iter().any(|active| {
                        active.position().partition() == retained.member.position().partition()
                    })
            })
            .count();
        let additions = active
            .iter()
            .filter(|active| {
                !self.members.iter().any(|retained| {
                    retained.broker_id == broker_id
                        && retained.member.position().partition() == active.position().partition()
                })
            })
            .count();
        if self
            .members
            .len()
            .saturating_sub(forgotten_count)
            .saturating_add(additions)
            > self.member_capacity
        {
            return Err((BrokerSessionError::MemberCapacity, active));
        }
        let mut forgotten = Vec::new();
        if forgotten.try_reserve_exact(forgotten_count).is_err() {
            return Err((BrokerSessionError::Allocation, active));
        }
        forgotten.extend(
            self.members
                .iter()
                .filter(|retained| {
                    retained.broker_id == broker_id
                        && retained.forgotten
                        && !active.iter().any(|active| {
                            active.position().partition() == retained.member.position().partition()
                        })
                })
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

    pub(super) fn try_begin_forgotten(
        &mut self,
    ) -> Result<Option<BrokerSessionPlan>, BrokerSessionError> {
        let Some(index) = self.entries.iter().position(|entry| {
            !entry.in_flight
                && entry.metadata.is_incremental()
                && self
                    .members
                    .iter()
                    .any(|member| member.broker_id == entry.broker_id && member.forgotten)
        }) else {
            return Ok(None);
        };
        let broker_id = self.entries[index].broker_id;
        let forgotten_count = self
            .members
            .iter()
            .filter(|member| member.broker_id == broker_id && member.forgotten)
            .count();
        let mut forgotten = Vec::new();
        forgotten
            .try_reserve_exact(forgotten_count)
            .map_err(|_error| BrokerSessionError::Allocation)?;
        forgotten.extend(
            self.members
                .iter()
                .filter(|member| member.broker_id == broker_id && member.forgotten)
                .map(|member| member.member.clone()),
        );
        self.entries[index].in_flight = true;
        Ok(Some(BrokerSessionPlan {
            broker_id,
            session: self.entries[index].metadata,
            active: Vec::new(),
            forgotten,
            close: false,
        }))
    }
}
