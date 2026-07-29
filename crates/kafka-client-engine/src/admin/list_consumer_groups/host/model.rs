//! Concrete operation, handoff, submission, and correlation ownership states.

use kafka_client_core::{
    AdminGroupListingFilters, AdminListConsumerGroupsMachine, AdminListConsumerGroupsState,
    AdminListConsumerGroupsTerminal, OperationId,
};

use crate::{
    clock::OperationDeadline,
    completion::CompletionId,
    driver::{ListConsumerGroupsCall, ListConsumerGroupsRawTerminal},
};

/// Exact discovery or broker call ready for driver admission.
pub(crate) enum ListConsumerGroupsSubmissionKind {
    Discovery,
    Broker {
        broker_id: i32,
        filters: AdminGroupListingFilters,
        retained_limit: usize,
    },
}

pub(crate) struct ListConsumerGroupsSubmission {
    pub(super) operation_id: OperationId,
    pub(super) deadline: OperationDeadline,
    pub(super) kind: ListConsumerGroupsSubmissionKind,
}

impl ListConsumerGroupsSubmission {
    pub(crate) fn into_parts(
        self,
    ) -> (
        OperationId,
        OperationDeadline,
        ListConsumerGroupsSubmissionKind,
    ) {
        (self.operation_id, self.deadline, self.kind)
    }
}

pub(crate) enum ListConsumerGroupsTurn {
    Idle,
    Progress,
    Submit(ListConsumerGroupsSubmission),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ListConsumerGroupsHandoff {
    Untouched,
    HandedOff,
    Submitted,
}

pub(super) struct ListConsumerGroupsOperation {
    pub(super) operation_id: OperationId,
    pub(super) machine: AdminListConsumerGroupsMachine,
    pub(super) completion_id: CompletionId,
    pub(super) deadline: OperationDeadline,
    pub(super) retained_bytes: usize,
    pub(super) remaining_result_bytes: usize,
    pub(super) submission: Option<ListConsumerGroupsSubmission>,
    pub(super) rejected_submission: Option<ListConsumerGroupsSubmissionKind>,
    pub(super) handoff: ListConsumerGroupsHandoff,
    pub(super) call: Option<ListConsumerGroupsCall>,
    pub(super) raw_terminal: Option<ListConsumerGroupsRawTerminal>,
    pub(super) terminal: Option<AdminListConsumerGroupsTerminal>,
}

impl ListConsumerGroupsOperation {
    pub(super) fn matches_submission(&self, kind: &ListConsumerGroupsSubmissionKind) -> bool {
        match (self.machine.state(), kind) {
            (
                AdminListConsumerGroupsState::AwaitingDiscoveryDriver,
                ListConsumerGroupsSubmissionKind::Discovery,
            ) => true,
            (
                AdminListConsumerGroupsState::AwaitingBrokerDriver,
                ListConsumerGroupsSubmissionKind::Broker {
                    broker_id,
                    filters,
                    retained_limit,
                },
            ) => {
                self.machine.current_broker() == Some(*broker_id)
                    && self.machine.filters() == filters
                    && self.remaining_result_bytes == *retained_limit
            }
            _ => false,
        }
    }

    pub(super) fn matches_call(&self, call: &ListConsumerGroupsCall) -> bool {
        match self.machine.state() {
            AdminListConsumerGroupsState::AwaitingDiscoveryDriver
            | AdminListConsumerGroupsState::DiscoverySubmitted => call.matches_discovery(),
            AdminListConsumerGroupsState::AwaitingBrokerDriver
            | AdminListConsumerGroupsState::BrokerSubmitted => {
                self.machine.current_broker().is_some_and(|broker_id| {
                    call.matches_broker(
                        broker_id,
                        self.machine.filters(),
                        self.remaining_result_bytes,
                    )
                })
            }
            _ => false,
        }
    }

    pub(super) fn matches_raw(&self, raw: &ListConsumerGroupsRawTerminal) -> bool {
        match self.machine.state() {
            AdminListConsumerGroupsState::DiscoverySubmitted => raw.matches_discovery(),
            AdminListConsumerGroupsState::BrokerSubmitted => {
                self.machine.current_broker().is_some_and(|broker_id| {
                    raw.matches_broker(
                        broker_id,
                        self.machine.filters(),
                        self.remaining_result_bytes,
                    )
                })
            }
            _ => false,
        }
    }
}
