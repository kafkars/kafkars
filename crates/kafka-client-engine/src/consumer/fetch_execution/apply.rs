//! Ordered core application, store authorization, and route confirmation.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerMachineError,
    AssignedConsumerTransition, FetchFence,
};

use super::{
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, RetainedFetchFault},
    prepared::PreparedFetchExecution,
    terminal::{FetchTerminalAction, FetchTerminalFact, TerminalStorage},
    terminal_proposal::{FetchTerminalProposal, PartitionOffsetOutOfRangeProposal},
};

impl DirectFetchExecutor {
    pub(in crate::consumer) fn discard_retired_terminal_proposal(
        &mut self,
        proposal: FetchTerminalProposal,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let fact = proposal.into_fact();
        self.discard_stale_terminal(fact.request, fact.storage, fact.session)
    }

    pub(in crate::consumer) fn apply_terminal_proposal(
        &mut self,
        machine: &mut AssignedConsumerMachine,
        proposal: FetchTerminalProposal,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        self.apply_terminal_with_input(machine, proposal, None)
    }

    pub(in crate::consumer) fn apply_offset_out_of_range_reset(
        &mut self,
        machine: &mut AssignedConsumerMachine,
        proposal: PartitionOffsetOutOfRangeProposal,
        input: kafka_client_core::AssignedConsumerInput,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        self.apply_terminal_with_input(machine, proposal.into_proposal(), Some(input))
    }

    fn apply_terminal_with_input(
        &mut self,
        machine: &mut AssignedConsumerMachine,
        proposal: FetchTerminalProposal,
        replacement: Option<kafka_client_core::AssignedConsumerInput>,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let fact = proposal.into_fact();
        let FetchTerminalFact {
            request,
            hard_output_bytes: _hard_output_bytes,
            action,
            storage,
            session,
        } = fact;
        let fence = request.fence();
        let input = match (replacement, action) {
            (Some(input), FetchTerminalAction::Apply(_terminal)) => input,
            (Some(_input), FetchTerminalAction::Reestablish { .. }) => {
                unreachable!("offset-out-of-range proof excludes session re-establishment")
            }
            (None, FetchTerminalAction::Apply(input)) => input,
            (None, FetchTerminalAction::Reestablish { hard_output_bytes }) => {
                return self.reestablish_broker_session(request, hard_output_bytes, storage);
            }
        };
        let transition = match machine.apply(input) {
            Ok(transition) => transition,
            Err(error) if stale_terminal(fence, &error) => {
                return self.discard_stale_terminal(request, storage, session);
            }
            Err(error) => {
                self.fault = Some(RetainedFetchFault::Request { _request: request });
                return Err(FetchExecutionError::Core(error));
            }
        };
        let store_result = match storage {
            TerminalStorage::Released => Ok(()),
            TerminalStorage::NonDelivery(stored) => self.store.discard_non_delivery(stored),
            TerminalStorage::Deliverable(stored, next_offset) => {
                let expected = AssignedConsumerEffect::AuthorizeFetchDelivery {
                    fence: stored,
                    next_offset,
                };
                if transition.effects().first() != Some(&expected) {
                    self.retain_transition(request, transition);
                    return Err(FetchExecutionError::UnexpectedDeliveryAuthorization { fence });
                }
                self.store.authorize(stored, next_offset)
            }
        };
        if let Err(error) = store_result {
            self.retain_transition(request, transition);
            return Err(FetchExecutionError::Store(error));
        }
        if let Err(error) = self.confirm_fetch_settlement(fence) {
            self.retain_transition(request, transition);
            return Err(FetchExecutionError::Confirm(error));
        }
        match self.complete_broker_session(fence, session) {
            Ok(true) => {}
            Ok(false) => self.commit_fetch_session(fence, session),
            Err(error) => {
                self.retain_transition(request, transition);
                return Err(error);
            }
        }
        Ok(Some(transition))
    }

    fn reestablish_broker_session(
        &mut self,
        request: crate::driver::PartitionFetchRequest,
        hard_output_bytes: usize,
        storage: TerminalStorage,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let fence = request.fence();
        let prepared = PreparedFetchExecution::from_parts(request, hard_output_bytes);
        if !matches!(storage, TerminalStorage::Released) {
            self.fault = Some(RetainedFetchFault::Prepared {
                _prepared: prepared,
            });
            return Err(FetchExecutionError::BrokerSession);
        }
        let Some(broker_id) = self.active_broker_sessions.iter().find_map(|active| {
            active
                .fences
                .contains(&fence)
                .then_some(active.plan.broker_id())
        }) else {
            self.fault = Some(RetainedFetchFault::Prepared {
                _prepared: prepared,
            });
            return Err(FetchExecutionError::BrokerSession);
        };
        if let Err(error) = self.confirm_fetch_settlement(fence) {
            self.fault = Some(RetainedFetchFault::Prepared {
                _prepared: prepared,
            });
            return Err(FetchExecutionError::Confirm(error));
        }
        match self.complete_broker_session(fence, crate::protocol::fetch::FetchSessionUpdate::Reset)
        {
            Ok(true) => self.restore_routed(broker_id, prepared),
            Ok(false) => {
                self.fault = Some(RetainedFetchFault::Prepared {
                    _prepared: prepared,
                });
                return Err(FetchExecutionError::BrokerSession);
            }
            Err(error) => {
                self.fault = Some(RetainedFetchFault::Prepared {
                    _prepared: prepared,
                });
                return Err(error);
            }
        }
        Ok(None)
    }

    fn discard_stale_terminal(
        &mut self,
        request: crate::driver::PartitionFetchRequest,
        storage: TerminalStorage,
        session: crate::protocol::fetch::FetchSessionUpdate,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let fence = request.fence();
        if !matches!(storage, TerminalStorage::Released)
            && let Err(error) = self.store.discard_stale(fence)
        {
            self.fault = Some(RetainedFetchFault::Request { _request: request });
            return Err(FetchExecutionError::Store(error));
        }
        if let Err(error) = self.confirm_fetch_settlement(fence) {
            self.fault = Some(RetainedFetchFault::Request { _request: request });
            return Err(FetchExecutionError::Confirm(error));
        }
        match self.complete_broker_session(fence, session) {
            Ok(true) => {}
            Ok(false) => self.commit_fetch_session(fence, session),
            Err(error) => {
                self.fault = Some(RetainedFetchFault::Request { _request: request });
                return Err(error);
            }
        }
        Ok(None)
    }

    pub(super) fn retain_transition(
        &mut self,
        request: crate::driver::PartitionFetchRequest,
        transition: AssignedConsumerTransition,
    ) {
        self.fault = Some(RetainedFetchFault::Transition {
            _request: request,
            _transition: transition,
        });
    }

    fn confirm_fetch_settlement(
        &mut self,
        fence: FetchFence,
    ) -> Result<(), crate::driver::FetchConfirmationError> {
        if self.broker_calls_are_active() {
            self.broker_calls.confirm_fetch_settlement(fence)
        } else {
            self.calls.confirm_fetch_settlement(fence)
        }
    }
}

fn stale_terminal(fence: FetchFence, error: &AssignedConsumerMachineError) -> bool {
    match error {
        AssignedConsumerMachineError::StaleAssignment { active, supplied } => {
            fence.position().assignment_epoch() == *supplied && supplied < active
        }
        AssignedConsumerMachineError::StalePosition { active, supplied } => {
            fence.position() == *supplied
                && supplied.assignment_epoch() == active.assignment_epoch()
                && supplied.partition() == active.partition()
                && supplied.position_epoch() < active.position_epoch()
        }
        AssignedConsumerMachineError::StaleFetch { supplied } => fence == *supplied,
        _ => false,
    }
}
