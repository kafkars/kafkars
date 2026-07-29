//! Driver fact normalization, core settlement, and terminal publication.

use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use kafka_client_core::{
    TransactionInitializationBrokerCategory, TransactionInitializationBrokerFailure,
    TransactionInitializationEffect, TransactionInitializationInput,
    TransactionInitializationTerminal,
};

use crate::{
    driver::{TransactionInitDriverFailureKind, TransactionInitTerminalFact},
    protocol::transaction::{
        TransactionInitBrokerCategory, TransactionInitResponseFailure,
        normalize_transaction_init_response,
    },
};

use super::{
    LiveTransactionalOwner, TRANSACTION_INITIALIZATION_OPERATION_BYTES,
    TransactionInitializationHost,
};
use crate::transaction::TransactionExecutionHost;
use crate::transaction::initialization::{
    RetainedTransactionInitializationOutcome, TransactionInitializationHostError,
    TransactionalOwnerParts,
};

impl TransactionInitializationHost {
    pub(super) fn poll_one_call(&mut self) -> Result<bool, TransactionInitializationHostError> {
        let Some(index) = self
            .operations
            .iter()
            .position(|operation| operation.call.is_some())
        else {
            return Ok(false);
        };
        let terminal = self.operations[index]
            .call
            .as_mut()
            .and_then(crate::driver::TransactionInitCall::try_terminal);
        let Some(terminal) = terminal else {
            return Ok(false);
        };
        drop(self.operations[index].call.take());
        let terminal =
            terminal.map_err(|_error| TransactionInitializationHostError::CallCompletion)?;
        let input = terminal_input(terminal.fact());
        self.operations[index].raw_terminal = Some(terminal);
        self.settle_raw(index, input)?;
        Ok(true)
    }

    fn settle_raw(
        &mut self,
        index: usize,
        input: TransactionInitializationInput,
    ) -> Result<(), TransactionInitializationHostError> {
        let owner_id = self.operations[index].owner_id;
        let operation_id = self.operations[index].operation_id;
        let transition = self.operations[index].machine.apply(owner_id, input)?;
        let raw = self.operations[index]
            .raw_terminal
            .take()
            .ok_or(TransactionInitializationHostError::MissingTerminal)?;
        raw.discard();
        match transition.into_effect() {
            Some(TransactionInitializationEffect::Complete {
                owner_id: effect_owner,
                operation_id: effect_operation,
                terminal,
            }) if effect_owner == owner_id && effect_operation == operation_id => {
                self.retain_initialization_outcome(index, terminal)?;
                self.publish_terminal(index)
            }
            _ => Err(TransactionInitializationHostError::UnexpectedEffect),
        }
    }

    pub(super) fn apply(
        &mut self,
        index: usize,
        input: TransactionInitializationInput,
    ) -> Result<(), TransactionInitializationHostError> {
        let owner_id = self.operations[index].owner_id;
        let transition = self.operations[index].machine.apply(owner_id, input)?;
        match transition.into_effect() {
            None => Ok(()),
            Some(TransactionInitializationEffect::Complete {
                owner_id: effect_owner,
                operation_id,
                terminal,
            }) if effect_owner == owner_id
                && operation_id == self.operations[index].operation_id =>
            {
                self.retain_initialization_outcome(index, terminal)?;
                self.publish_terminal(index)
            }
            _ => Err(TransactionInitializationHostError::UnexpectedEffect),
        }
    }

    fn retain_initialization_outcome(
        &mut self,
        index: usize,
        terminal: TransactionInitializationTerminal,
    ) -> Result<(), TransactionInitializationHostError> {
        match terminal {
            TransactionInitializationTerminal::Initialized(identity) => {
                let request = self.operations[index]
                    .request
                    .take()
                    .ok_or(TransactionInitializationHostError::MissingTerminal)?;
                let (transactional_id, transaction_timeout_ms) = request.into_parts();
                let transactional_id = Arc::<str>::from(transactional_id);
                let active = Arc::new(AtomicBool::new(true));
                let owner_id = self.operations[index].owner_id;
                let publisher = self.completion_owner.lifecycle_publisher()?;
                let send_publisher = self.completion_owner.send_publisher()?;
                let offset_commit_publisher = self.completion_owner.offset_commit_publisher()?;
                let parts = TransactionalOwnerParts::new(
                    owner_id,
                    Arc::clone(&transactional_id),
                    identity.producer_id(),
                    identity.producer_epoch(),
                    Arc::clone(&active),
                    self.release_sender.clone(),
                    publisher,
                    send_publisher,
                    offset_commit_publisher,
                );
                let execution =
                    match TransactionExecutionHost::try_new(parts, self.execution_limits) {
                        Ok(execution) => execution,
                        Err((_error, parts)) => {
                            parts.discard_uninstalled();
                            self.operations[index].terminal = Some(
                            crate::transaction::initialization::outcome::
                                execution_unavailable_retained_outcome(),
                        );
                            return Ok(());
                        }
                    };
                self.executions.push(execution);
                self.live_owners.push(LiveTransactionalOwner {
                    owner_id,
                    active: Arc::clone(&active),
                    retained_bytes: TRANSACTION_INITIALIZATION_OPERATION_BYTES,
                });
                self.operations[index].terminal =
                    Some(RetainedTransactionInitializationOutcome::initialized(
                        owner_id,
                        transactional_id,
                        identity.producer_id(),
                        identity.producer_epoch(),
                        active,
                        self.owner_loss_sender.clone(),
                        Duration::from_millis(u64::from(transaction_timeout_ms)),
                    ));
                Ok(())
            }
            failed @ TransactionInitializationTerminal::Failed(_) => {
                self.operations[index].terminal =
                    crate::transaction::initialization::outcome::failed_retained_outcome(failed);
                Ok(())
            }
        }
    }

    pub(super) fn publish_terminal(
        &mut self,
        index: usize,
    ) -> Result<(), TransactionInitializationHostError> {
        let terminal = self.operations[index]
            .terminal
            .take()
            .ok_or(TransactionInitializationHostError::MissingTerminal)?;
        let initialized = terminal.is_initialized();
        let completion_id = self.operations[index].completion_id;
        match self.completions.publish(completion_id, terminal) {
            Ok(()) => {
                self.operations.remove(index);
                self.published_bytes.push((
                    completion_id,
                    if initialized {
                        0
                    } else {
                        TRANSACTION_INITIALIZATION_OPERATION_BYTES
                    },
                ));
                Ok(())
            }
            Err((error, terminal)) => {
                self.operations[index].terminal = Some(terminal);
                Err(TransactionInitializationHostError::Completion(error))
            }
        }
    }
}

fn terminal_input(fact: TransactionInitTerminalFact<'_>) -> TransactionInitializationInput {
    match fact {
        TransactionInitTerminalFact::Failed { kind, delivery } => match kind {
            TransactionInitDriverFailureKind::DeadlineElapsed => {
                TransactionInitializationInput::DriverDeadlineElapsed { delivery }
            }
            TransactionInitDriverFailureKind::InvalidResponse => {
                TransactionInitializationInput::InvalidResponse
            }
            TransactionInitDriverFailureKind::Compatibility => {
                TransactionInitializationInput::TransportFailed { delivery }
            }
            TransactionInitDriverFailureKind::Transport => {
                TransactionInitializationInput::TransportFailed { delivery }
            }
        },
        TransactionInitTerminalFact::Response { response, .. } => {
            match normalize_transaction_init_response(response) {
                Ok(identity) => TransactionInitializationInput::BrokerInitialized {
                    producer_id: identity.producer_id,
                    producer_epoch: identity.producer_epoch,
                },
                Err(TransactionInitResponseFailure::Broker { code, category }) => {
                    TransactionInitializationInput::BrokerRejected {
                        failure: TransactionInitializationBrokerFailure::new(
                            code,
                            match category {
                                TransactionInitBrokerCategory::Fenced => {
                                    TransactionInitializationBrokerCategory::Fenced
                                }
                                TransactionInitBrokerCategory::Rejected => {
                                    TransactionInitializationBrokerCategory::Rejected
                                }
                            },
                        ),
                    }
                }
                Err(TransactionInitResponseFailure::InvalidIdentity) => {
                    TransactionInitializationInput::InvalidResponse
                }
            }
        }
    }
}
