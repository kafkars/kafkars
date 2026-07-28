//! Declarative facade for assignment-fenced classic-group Fetch activation ownership.

mod activation;
mod delivery;
mod model;
mod owner;
mod owner_observation;
mod position_transfer;
mod prepare;
mod recovery;
mod retirement;
mod turn;
mod turn_model;

#[cfg(test)]
mod activation_test;
#[cfg(test)]
mod delivery_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod position_failure_transfer_test;
#[cfg(test)]
mod position_transfer_test;
#[cfg(test)]
mod prepare_backpressure_test;
#[cfg(test)]
mod prepare_control_test;
#[cfg(test)]
mod prepare_ready_test;
#[cfg(test)]
mod recovery_test;
#[cfg(test)]
mod retirement_test;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod turn_test;

#[cfg(test)]
pub(super) use activation::{
    ClassicGroupFetchActivationErrorKind, ClassicGroupFetchActivationFailureKind,
    ClassicGroupFetchActivationFault, ClassicGroupFetchPostCoreFaultKind,
};
pub(super) use delivery::ClassicGroupFetchReclaimError;
pub(in crate::consumer) use delivery::{ClassicGroupFetchDelivery, ClassicGroupFetchDeliveryError};
pub(super) use model::{
    ClassicGroupFetchBuildError, ClassicGroupFetchOwnerFault, ClassicGroupFetchOwnerFaultKind,
};
#[cfg(test)]
pub(super) use model::{
    ClassicGroupFetchCapturedFailure, ClassicGroupFetchFront, ClassicGroupFetchPreflightError,
};
pub(super) use owner::ClassicGroupFetchOwner;
pub(super) use position_transfer::{
    ClassicGroupFetchCurrentFenceError, ClassicGroupFetchTransferError,
    ClassicGroupFetchTransferTurn, current_position_fence, transfer_completed_position,
};
pub(super) use recovery::ClassicGroupFetchShutdownRecovery;
pub(super) use retirement::{ClassicGroupFetchRetirement, ClassicGroupFetchRetirementError};
#[cfg(test)]
pub(in crate::consumer::group) use test_support::{
    completed_ready, install_ready_delivery_for_test,
};
