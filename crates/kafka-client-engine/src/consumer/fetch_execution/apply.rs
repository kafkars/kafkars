//! Ordered core application, store authorization, and route confirmation.

use kafka_client_core::{
    AssignedConsumerEffect, AssignedConsumerMachine, AssignedConsumerMachineError,
    AssignedConsumerTransition, FetchFence,
};

use super::{
    executor::DirectFetchExecutor,
    fault::{FetchExecutionError, RetainedFetchFault},
    terminal::{FetchTerminalFact, TerminalStorage},
};

impl DirectFetchExecutor {
    pub(super) fn apply_terminal(
        &mut self,
        machine: &mut AssignedConsumerMachine,
        fact: FetchTerminalFact,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let fence = fact.request.fence();
        let transition = match machine.apply(fact.input) {
            Ok(transition) => transition,
            Err(error) if stale_terminal(fence, &error) => {
                return self.discard_stale_terminal(fact.request, fact.storage);
            }
            Err(error) => {
                self.fault = Some(RetainedFetchFault::Request {
                    _request: fact.request,
                });
                return Err(FetchExecutionError::Core(error));
            }
        };
        let store_result = match fact.storage {
            TerminalStorage::Released => Ok(()),
            TerminalStorage::NonDelivery(stored) => self.store.discard_non_delivery(stored),
            TerminalStorage::Deliverable(stored, next_offset) => {
                let expected = AssignedConsumerEffect::AuthorizeFetchDelivery {
                    fence: stored,
                    next_offset,
                };
                if transition.effects().first() != Some(&expected) {
                    self.retain_transition(fact.request, transition);
                    return Err(FetchExecutionError::UnexpectedDeliveryAuthorization { fence });
                }
                self.store.authorize(stored, next_offset)
            }
        };
        if let Err(error) = store_result {
            self.retain_transition(fact.request, transition);
            return Err(FetchExecutionError::Store(error));
        }
        if let Err(error) = self.calls.confirm_fetch_settlement(fence) {
            self.retain_transition(fact.request, transition);
            return Err(FetchExecutionError::Confirm(error));
        }
        Ok(Some(transition))
    }

    fn discard_stale_terminal(
        &mut self,
        request: crate::driver::PartitionFetchRequest,
        storage: TerminalStorage,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let fence = request.fence();
        if !matches!(storage, TerminalStorage::Released)
            && let Err(error) = self.store.discard_stale(fence)
        {
            self.fault = Some(RetainedFetchFault::Request { _request: request });
            return Err(FetchExecutionError::Store(error));
        }
        if let Err(error) = self.calls.confirm_fetch_settlement(fence) {
            self.fault = Some(RetainedFetchFault::Request { _request: request });
            return Err(FetchExecutionError::Confirm(error));
        }
        Ok(None)
    }

    fn retain_transition(
        &mut self,
        request: crate::driver::PartitionFetchRequest,
        transition: AssignedConsumerTransition,
    ) {
        self.fault = Some(RetainedFetchFault::Transition {
            _request: request,
            _transition: transition,
        });
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
