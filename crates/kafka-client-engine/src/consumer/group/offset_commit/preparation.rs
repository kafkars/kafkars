//! Allocation-free binding of a core admission to pre-core protocol owners.

use kafka_client_core::{
    GroupOffsetCommitEffect, GroupOffsetCommitMachine, GroupOffsetCommitTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::{CompletionId, CompletionObserver},
    protocol::consumer::{
        GroupOffsetCommitEntryReservation, GroupOffsetCommitPreparationError,
        GroupOffsetCommitResultReservation, PreparedGroupOffsetCommit,
        PreparedGroupOffsetCommitRequest,
    },
};

use super::{
    host::{
        AcceptedGroupOffsetCommit, GROUP_OFFSET_COMMIT_OPERATION_BYTES, GroupOffsetCommitAttempt,
        GroupOffsetCommitHost, GroupOffsetCommitHostError, GroupOffsetCommitOperation,
        GroupOffsetCommitSubmission,
    },
    snapshot::PreparedSnapshot,
};

pub(super) enum HostPreparation {
    Ready {
        prepared: PreparedGroupOffsetCommit,
        request: PreparedGroupOffsetCommitRequest,
    },
    Fault {
        error: GroupOffsetCommitPreparationError,
        request: PreparedGroupOffsetCommitRequest,
    },
}

pub(super) struct InstalledPreparation {
    pub(super) attempt: Option<GroupOffsetCommitAttempt>,
    pub(super) terminal: Option<GroupOffsetCommitTerminal>,
    pub(super) byte_charge: usize,
    pub(super) fault: Option<GroupOffsetCommitHostError>,
}

pub(super) enum PreparationOutcome {
    Installed(InstalledPreparation),
    RetainedFault {
        error: GroupOffsetCommitHostError,
        preparation: HostPreparation,
    },
}

impl GroupOffsetCommitHost {
    #[allow(
        clippy::too_many_arguments,
        reason = "one exact admission ownership transfer"
    )]
    pub(super) fn install_admitted(
        &mut self,
        operation_id: OperationId,
        deadline: OperationDeadline,
        mut machine: GroupOffsetCommitMachine,
        effect: GroupOffsetCommitEffect,
        snapshot: PreparedSnapshot,
        entry_capacity: GroupOffsetCommitEntryReservation,
        result_capacity: GroupOffsetCommitResultReservation,
        completion_id: CompletionId,
        observer: CompletionObserver<GroupOffsetCommitTerminal>,
    ) -> Result<AcceptedGroupOffsetCommit, super::admission::GroupOffsetCommitAdmissionFailure>
    {
        let preparation = match PreparedGroupOffsetCommit::from_effect(
            effect,
            deadline,
            snapshot.session,
            snapshot.topic_names,
            entry_capacity,
            result_capacity,
        ) {
            Ok(prepared) => HostPreparation::Ready {
                prepared,
                request: snapshot.request,
            },
            Err(error) => HostPreparation::Fault {
                error,
                request: snapshot.request,
            },
        };
        let installed = self.finish_preparation(operation_id, &mut machine, preparation);
        let installed = match installed {
            PreparationOutcome::Installed(installed) => installed,
            PreparationOutcome::RetainedFault { error, preparation } => {
                self.retain_preparation_fault(preparation);
                self.fault = Some(error);
                InstalledPreparation {
                    attempt: None,
                    terminal: None,
                    byte_charge: GROUP_OFFSET_COMMIT_OPERATION_BYTES,
                    fault: Some(error),
                }
            }
        };
        self.operations.push(GroupOffsetCommitOperation {
            operation_id,
            machine,
            completion_id,
            deadline,
            attempt: installed.attempt,
            terminal: installed.terminal,
            byte_charge: installed.byte_charge,
        });
        if self
            .operations
            .last()
            .is_some_and(|operation| operation.terminal.is_some())
            && let Err(error) = self.publish_terminal(self.operations.len() - 1)
        {
            if !matches!(
                error,
                GroupOffsetCommitHostError::Completion(
                    crate::completion::CompletionRegistryError::NotificationBackpressure
                )
            ) {
                self.fault = Some(error);
            }
            return Ok(AcceptedGroupOffsetCommit {
                observer,
                fault: Some(error),
            });
        }
        Ok(AcceptedGroupOffsetCommit {
            observer,
            fault: installed.fault,
        })
    }

    fn finish_preparation(
        &mut self,
        operation_id: OperationId,
        machine: &mut GroupOffsetCommitMachine,
        preparation: HostPreparation,
    ) -> PreparationOutcome {
        match preparation {
            HostPreparation::Ready { prepared, request } => {
                let Some(bytes) = Self::actual_operation_bytes(machine, &prepared, &request) else {
                    return self.settle_preparation_failure(
                        operation_id,
                        machine,
                        HostPreparation::Ready { prepared, request },
                        GroupOffsetCommitHostError::ByteAccounting,
                    );
                };
                if bytes > GROUP_OFFSET_COMMIT_OPERATION_BYTES {
                    return self.settle_preparation_failure(
                        operation_id,
                        machine,
                        HostPreparation::Ready { prepared, request },
                        GroupOffsetCommitHostError::ByteAccounting,
                    );
                }
                let Some(retained) = self
                    .retained_bytes
                    .checked_sub(GROUP_OFFSET_COMMIT_OPERATION_BYTES)
                    .and_then(|retained| retained.checked_add(bytes))
                else {
                    return PreparationOutcome::RetainedFault {
                        error: GroupOffsetCommitHostError::ByteAccounting,
                        preparation: HostPreparation::Ready { prepared, request },
                    };
                };
                self.retained_bytes = retained;
                PreparationOutcome::Installed(InstalledPreparation {
                    attempt: Some(GroupOffsetCommitAttempt::Queued(
                        GroupOffsetCommitSubmission { prepared, request },
                    )),
                    terminal: None,
                    byte_charge: bytes,
                    fault: None,
                })
            }
            preparation @ HostPreparation::Fault { .. } => self.settle_preparation_failure(
                operation_id,
                machine,
                preparation,
                GroupOffsetCommitHostError::Preparation,
            ),
        }
    }
}
