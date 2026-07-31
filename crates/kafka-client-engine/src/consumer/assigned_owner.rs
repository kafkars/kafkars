//! Unique composition root for one standalone directly assigned consumer.

use std::{collections::VecDeque, sync::Arc};

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerTransition, Deadline,
};

use crate::clock::MonotonicClock;
use crate::completion::CompletionRegistry;

use super::{
    assigned_close_slot::AssignedCloseSlot,
    assigned_event::AssignedConsumerEventStore,
    assigned_host::{AssignedConsumerClosePublisher, AssignedConsumerCloseTerminal},
    assigned_owner_fault::{AssignedConsumerFaultKind, AssignedConsumerOwnerFault},
    assigned_owner_model::{
        AssignedConsumerOwnerBuildError, AssignedConsumerOwnerLimits,
        AssignedConsumerOwnerSettings, PendingPosition, RawPositionDeadline, minimum_deadline,
    },
    assigned_timers::AssignedTimers,
    assigned_topics::AssignedTopics,
    fetch_execution::{DirectFetchExecutor, FetchReclaimFailure, PreparedFetchExecution},
    position_execution::PositionResolutionExecutor,
};

#[cfg(test)]
use super::assigned_owner_test::AssignedConsumerNotifierGuard;

/// Sole engine owner of one core machine and every retained execution mechanism.
pub(crate) struct AssignedConsumerOwner {
    pub(super) machine: AssignedConsumerMachine,
    pub(super) topics: AssignedTopics,
    pub(super) timers: AssignedTimers,
    pub(super) positions: PositionResolutionExecutor,
    pub(super) fetches: DirectFetchExecutor,
    pub(super) events: AssignedConsumerEventStore,
    pub(super) close: AssignedCloseSlot,
    pub(super) close_completions:
        CompletionRegistry<AssignedConsumerCloseTerminal, AssignedConsumerClosePublisher>,
    pub(super) clock: Arc<MonotonicClock>,
    pub(super) settings: AssignedConsumerOwnerSettings,
    pub(super) limits: AssignedConsumerOwnerLimits,
    pub(super) effects: VecDeque<AssignedConsumerEffect>,
    pub(super) raw_position_deadlines: VecDeque<RawPositionDeadline>,
    pub(super) pending_positions: VecDeque<PendingPosition>,
    pub(super) pending_fetches: VecDeque<PreparedFetchExecution>,
    pub(super) reclaim_faults: Vec<FetchReclaimFailure>,
    pub(super) reclaim_overflow: Option<FetchReclaimFailure>,
    pub(super) fault: Option<AssignedConsumerOwnerFault>,
    #[cfg(test)]
    pub(super) close_notifier: Option<AssignedConsumerNotifierGuard>,
    #[cfg(test)]
    pub(super) close_publish_faults: VecDeque<crate::completion::CompletionRegistryError>,
}

impl AssignedConsumerOwner {
    pub(super) fn new(
        clock: Arc<MonotonicClock>,
        settings: AssignedConsumerOwnerSettings,
        limits: AssignedConsumerOwnerLimits,
        close_publisher: AssignedConsumerClosePublisher,
    ) -> Result<Self, AssignedConsumerOwnerBuildError> {
        if settings.due_timer_budget == 0 {
            return Err(AssignedConsumerOwnerBuildError::ZeroTimerBudget);
        }
        let mut effects = VecDeque::new();
        let mut raw_position_deadlines = VecDeque::new();
        let mut pending_positions = VecDeque::new();
        let mut pending_fetches = VecDeque::new();
        let mut reclaim_faults = Vec::new();
        effects
            .try_reserve_exact(limits.effect_capacity)
            .map_err(|_error| AssignedConsumerOwnerBuildError::Allocation)?;
        raw_position_deadlines
            .try_reserve_exact(limits.partition_capacity)
            .map_err(|_error| AssignedConsumerOwnerBuildError::Allocation)?;
        pending_positions
            .try_reserve_exact(limits.partition_capacity)
            .map_err(|_error| AssignedConsumerOwnerBuildError::Allocation)?;
        pending_fetches
            .try_reserve_exact(limits.partition_capacity)
            .map_err(|_error| AssignedConsumerOwnerBuildError::Allocation)?;
        reclaim_faults
            .try_reserve_exact(limits.delivery_capacity)
            .map_err(|_error| AssignedConsumerOwnerBuildError::Allocation)?;
        let mut fetches = DirectFetchExecutor::create_unbound(
            limits.call_capacity,
            limits.delivery_capacity,
            limits.delivery_bytes,
        );
        fetches
            .try_enable_sessions(limits.partition_capacity)
            .map_err(|()| AssignedConsumerOwnerBuildError::Allocation)?;
        fetches.configure_broker_session_close(
            settings.fetch_settings,
            settings.fetch_attempt_timeout,
        );
        Ok(Self {
            machine: AssignedConsumerMachine::with_read_isolation(settings.read_isolation),
            topics: AssignedTopics::new(limits.topic_limits),
            timers: AssignedTimers::new(limits.partition_capacity),
            positions: PositionResolutionExecutor::new(limits.call_capacity),
            fetches,
            events: AssignedConsumerEventStore::new(limits.partition_capacity)
                .map_err(AssignedConsumerOwnerBuildError::Event)?,
            close: AssignedCloseSlot::create_for_assigned_owner(),
            close_completions: CompletionRegistry::with_publisher(1, close_publisher),
            clock,
            settings,
            limits,
            effects,
            raw_position_deadlines,
            pending_positions,
            pending_fetches,
            reclaim_faults,
            reclaim_overflow: None,
            fault: None,
            #[cfg(test)]
            close_notifier: None,
            #[cfg(test)]
            close_publish_faults: VecDeque::new(),
        })
    }

    pub(crate) fn next_deadline(&self) -> Option<Deadline> {
        if self.fault.is_some()
            || !self.reclaim_faults.is_empty()
            || self.reclaim_overflow.is_some()
        {
            return None;
        }
        let mut next = self.timers.next_deadline();
        for deadline in &self.raw_position_deadlines {
            next = minimum_deadline(next, deadline.deadline.core());
        }
        for pending in &self.pending_positions {
            next = minimum_deadline(next, pending.deadline.core());
        }
        for pending in &self.pending_fetches {
            next = minimum_deadline(next, pending.deadline());
        }
        if let Some(deadline) = self.fetches.broker_session_close_deadline() {
            next = minimum_deadline(next, deadline);
        }
        next
    }

    pub(crate) const fn is_faulted(&self) -> bool {
        self.fault.is_some() || !self.reclaim_faults.is_empty() || self.reclaim_overflow.is_some()
    }

    pub(crate) fn fault_kind(&self) -> Option<AssignedConsumerFaultKind> {
        self.fault
            .as_ref()
            .map(AssignedConsumerOwnerFault::kind)
            .or_else(|| {
                (!self.reclaim_faults.is_empty() || self.reclaim_overflow.is_some())
                    .then_some(AssignedConsumerFaultKind::Reclaim)
            })
    }

    pub(super) fn retain_transition(
        &mut self,
        transition: AssignedConsumerTransition,
        position_deadline: Option<crate::clock::OperationDeadline>,
    ) {
        self.fault = Some(AssignedConsumerOwnerFault::Transition {
            transition,
            position_deadline,
        });
    }

    pub(super) fn enqueue_transition(
        &mut self,
        transition: AssignedConsumerTransition,
        position_deadline: Option<crate::clock::OperationDeadline>,
    ) {
        let effects_len = transition.effects().len();
        let resolution_count = transition
            .effects()
            .iter()
            .filter(|effect| matches!(effect, AssignedConsumerEffect::ResolvePosition { .. }))
            .count();
        let effect_fits = self
            .effects
            .len()
            .checked_add(effects_len)
            .is_some_and(|needed| needed <= self.limits.effect_capacity);
        let deadline_fits = self
            .raw_position_deadlines
            .len()
            .checked_add(resolution_count)
            .is_some_and(|needed| needed <= self.limits.partition_capacity);
        if !effect_fits || !deadline_fits || (resolution_count > 0 && position_deadline.is_none()) {
            self.retain_transition(transition, position_deadline);
            return;
        }
        if let Some(operation) = position_deadline {
            for effect in transition.effects() {
                if let AssignedConsumerEffect::ResolvePosition { fence, .. } = effect {
                    self.raw_position_deadlines.push_back(RawPositionDeadline {
                        fence: *fence,
                        deadline: operation,
                    });
                }
            }
        }
        for effect in transition.into_effects() {
            self.effects.push_back(effect);
        }
    }

    #[cfg(test)]
    pub(crate) fn install_fault_for_test(&mut self) {
        self.fault = Some(AssignedConsumerOwnerFault::Clock(
            crate::clock::ClockError::TickOverflow,
        ));
    }

    #[cfg(test)]
    pub(crate) fn install_ready_delivery_for_test(&mut self, record_offset: i64) {
        super::assigned_owner_close_test::install_pending_ready(self, record_offset);
    }

    #[cfg(test)]
    pub(crate) fn pending_effect_count_for_test(&self) -> usize {
        self.effects.len()
    }
}
