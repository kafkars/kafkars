//! Declarative facade for classic-group committed-position execution ownership.

mod activation;
mod close;
mod preparation;
mod preparation_input;
mod recovery;
mod recovery_fault;
mod registry_recovery;
mod registry_settlement;
mod registry_submission;
mod registry_turn;
mod settlement;
mod state;
mod state_execution;
mod submission;
mod submission_resolution;
mod terminal_application;
mod terminal_normalization;

pub(super) use activation::{
    ClassicGroupPositionActivationError, prepare_classic_group_fetch_activation,
};
pub(super) use close::ClassicGroupPositionCloseTurn;
#[cfg(test)]
pub(super) use preparation::CLASSIC_GROUP_POSITION_REQUEST_RETAINED_BYTES;
pub(super) use preparation::{
    CLASSIC_GROUP_POSITION_RESULT_RETAINED_BYTES, ClassicGroupPositionPreparation,
    ClassicGroupPositionPreparationError, ClassicGroupPositionPreparationMismatch,
    prepare_classic_group_position,
};
pub(super) use recovery_fault::ClassicGroupPositionRecoveryFault;
#[cfg(test)]
pub(super) use registry_settlement::ClassicGroupPositionSettlementTurn;
#[cfg(test)]
pub(super) use registry_submission::ClassicGroupPositionSubmissionTurn;
pub(super) use registry_turn::GroupConsumerPositionTurn;
pub(super) use state::{
    ClassicGroupPositionCompleted, ClassicGroupPositionConfirmationPending,
    ClassicGroupPositionDriverOwned, ClassicGroupPositionHandoff, ClassicGroupPositionPrepared,
};
pub(super) use state_execution::{
    ClassicGroupPositionExecution, ClassicGroupPositionExecutionState,
};
pub(super) use submission::{
    ClassicGroupPositionAcceptanceFailure, ClassicGroupPositionExecutionError,
    ClassicGroupPositionRejectionFailure,
};
pub(super) use terminal_application::ClassicGroupPositionTerminalApplicationFailure;

#[cfg(test)]
mod activation_test;
#[cfg(test)]
mod close_blocked_test;
#[cfg(test)]
mod close_test;
#[cfg(test)]
mod host_test;
#[cfg(test)]
mod preparation_test;
#[cfg(test)]
mod recovery_fault_test;
#[cfg(test)]
mod recovery_test;
#[cfg(test)]
mod settlement_confirmation_test;
#[cfg(test)]
mod settlement_correlation_test;
#[cfg(test)]
mod settlement_failure_test;
#[cfg(test)]
mod settlement_success_test;
#[cfg(test)]
mod settlement_test_support;
#[cfg(test)]
mod state_test;
#[cfg(test)]
mod submission_duplicate_test;
#[cfg(test)]
mod submission_test;
#[cfg(test)]
mod sync_install_failure_test;
#[cfg(test)]
mod terminal_application_test;
#[cfg(test)]
pub(in crate::consumer::group) mod test_support;
