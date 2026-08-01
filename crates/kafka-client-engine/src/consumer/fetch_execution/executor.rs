//! Unique bounded owner joining Fetch calls, output reservations, and delivery.

use crate::driver::{TrackedBrokerFetchCalls, TrackedFetchCalls};
use crate::protocol::fetch::FetchRequestSettings;
use kafka_client_core::{AssignedConsumerEffect, FetchFence};

use super::{
    super::fetch_store::{FetchDeliveryStore, FetchStoreReservation},
    broker_close::{ActiveBrokerSessionClose, BrokerSessionPolicy},
    broker_execution::{ActiveBrokerSession, PendingBrokerRoute, RoutedBrokerFetch},
    broker_maintenance_state::BrokerSessionMaintenance,
    broker_session::BrokerFetchSessions,
    fault::RetainedFetchFault,
    partition_session::DirectFetchSession,
};

pub(super) struct ActiveFetchReservation {
    pub(super) fence: FetchFence,
    pub(super) reservation: FetchStoreReservation,
}

/// Concrete direct-assignment Fetch interpreter.
pub(crate) struct DirectFetchExecutor {
    _seal: ExecutorSeal,
    pub(super) calls: TrackedFetchCalls,
    pub(super) broker_calls: TrackedBrokerFetchCalls,
    pub(super) store: FetchDeliveryStore,
    pub(super) active: Vec<ActiveFetchReservation>,
    pub(super) session_capacity: usize,
    pub(super) sessions: Vec<DirectFetchSession>,
    pub(super) broker_sessions: Option<BrokerFetchSessions>,
    pub(super) route_capacity: usize,
    pub(super) route_calls: Vec<PendingBrokerRoute>,
    pub(super) routed: Vec<RoutedBrokerFetch>,
    pub(super) active_broker_sessions: Vec<ActiveBrokerSession>,
    pub(super) broker_maintenance: Option<BrokerSessionMaintenance>,
    pub(super) broker_session_policy: Option<BrokerSessionPolicy>,
    pub(super) broker_maintenance_deferred: bool,
    pub(super) broker_close_requested: bool,
    pub(super) broker_close_deadline: Option<crate::clock::OperationDeadline>,
    pub(super) active_broker_close: Option<ActiveBrokerSessionClose>,
    pub(super) fault: Option<RetainedFetchFault>,
}

struct ExecutorSeal;

impl DirectFetchExecutor {
    pub(super) fn sessions_are_broker_routed(&self) -> bool {
        self.broker_sessions.is_some()
    }

    pub(crate) fn create_unbound(
        call_capacity: usize,
        delivery_capacity: usize,
        max_bytes: usize,
    ) -> Self {
        Self {
            _seal: ExecutorSeal,
            calls: TrackedFetchCalls::new(call_capacity),
            broker_calls: TrackedBrokerFetchCalls::new(call_capacity),
            store: FetchDeliveryStore::new(delivery_capacity, max_bytes),
            active: Vec::new(),
            session_capacity: 0,
            sessions: Vec::new(),
            broker_sessions: None,
            route_capacity: 0,
            route_calls: Vec::new(),
            routed: Vec::new(),
            active_broker_sessions: Vec::new(),
            broker_maintenance: None,
            broker_session_policy: None,
            broker_maintenance_deferred: false,
            broker_close_requested: false,
            broker_close_deadline: None,
            active_broker_close: None,
            fault: None,
        }
    }

    pub(crate) fn try_enable_sessions(&mut self, session_capacity: usize) -> Result<(), ()> {
        self.sessions
            .try_reserve_exact(session_capacity)
            .map_err(|_error| ())?;
        self.session_capacity = session_capacity;
        self.route_calls
            .try_reserve_exact(session_capacity)
            .map_err(|_error| ())?;
        self.routed
            .try_reserve_exact(session_capacity)
            .map_err(|_error| ())?;
        self.active_broker_sessions
            .try_reserve_exact(session_capacity)
            .map_err(|_error| ())?;
        let member_capacity = session_capacity.checked_mul(2).ok_or(())?;
        self.broker_sessions = Some(
            BrokerFetchSessions::try_new(session_capacity, member_capacity).map_err(|_error| ())?,
        );
        self.route_capacity = session_capacity;
        Ok(())
    }

    pub(crate) fn configure_broker_sessions(
        &mut self,
        settings: FetchRequestSettings,
        timeout: std::time::Duration,
    ) {
        self.broker_session_policy = Some(BrokerSessionPolicy { settings, timeout });
    }

    pub(super) fn reset_fetch_session_for_control(&mut self, effect: AssignedConsumerEffect) {
        self.sessions.retain(|session| {
            let position = session.position;
            !match effect {
                AssignedConsumerEffect::Revoke {
                    assignment_epoch,
                    partition,
                } => {
                    position.assignment_epoch() == assignment_epoch
                        && position.partition() == partition
                }
                AssignedConsumerEffect::Suspend { fence } => {
                    position.assignment_epoch() == fence.assignment_epoch()
                        && position.partition() == fence.partition()
                        && position.position_epoch() < fence.position_epoch()
                }
                _ => false,
            }
        });
    }

    pub(super) fn active_index(&self, fence: FetchFence) -> Option<usize> {
        self.active
            .iter()
            .position(|reservation| reservation.fence == fence)
    }

    pub(super) fn broker_calls_are_active(&self) -> bool {
        self.sessions_are_broker_routed() && self.calls.retained_count() == 0
    }

    pub(super) fn take_active(&mut self, index: usize) -> ActiveFetchReservation {
        self.active.swap_remove(index)
    }

    pub(crate) fn retained(&self) -> (usize, usize, usize) {
        let (deliveries, bytes) = self.store.retained();
        (
            self.calls
                .retained_count()
                .saturating_add(self.broker_calls.retained_count())
                .saturating_add(self.route_calls.len())
                .saturating_add(self.routed.len())
                .saturating_add(usize::from(self.broker_maintenance.is_some()))
                .saturating_add(usize::from(self.active_broker_close.is_some())),
            deliveries,
            bytes,
        )
    }

    pub(crate) fn broker_sessions_are_closed(&self) -> bool {
        self.active_broker_close.is_none()
            && self
                .broker_sessions
                .as_ref()
                .is_none_or(BrokerFetchSessions::is_empty)
    }

    pub(crate) fn broker_sessions_have_forgotten_ready(&self) -> bool {
        self.broker_sessions
            .as_ref()
            .is_some_and(BrokerFetchSessions::has_forgotten_ready)
    }

    pub(crate) fn defer_broker_session_maintenance(&mut self) {
        self.broker_maintenance_deferred = true;
    }

    pub(crate) fn resume_broker_session_maintenance(&mut self) {
        self.broker_maintenance_deferred = false;
    }

    pub(crate) fn retained_broker_sessions(&self) -> usize {
        self.broker_sessions
            .as_ref()
            .map_or(0, BrokerFetchSessions::len)
    }

    pub(super) fn can_reserve_broker_batch(&self, count: usize, bytes: usize) -> bool {
        self.store.can_reserve_batch_shape(count, bytes)
    }

    #[cfg(test)]
    pub(crate) fn reserve_output_for_test(
        &mut self,
        fence: FetchFence,
        bytes: usize,
    ) -> Result<(), super::super::fetch_store::FetchStoreFailure> {
        let reservation = self.store.try_reserve(fence, bytes)?;
        self.active
            .push(ActiveFetchReservation { fence, reservation });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn tracked_calls_for_test(&mut self) -> &mut TrackedFetchCalls {
        &mut self.calls
    }

    #[cfg(test)]
    pub(crate) fn install_fault_for_test(&mut self) {
        self.fault = Some(RetainedFetchFault::Staged);
    }
}
