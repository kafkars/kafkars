//! Declarative facade for assignment-fenced classic-group Fetch activation ownership.

mod activation;
mod model;
mod owner;
mod prepare;

#[cfg(test)]
mod activation_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod prepare_backpressure_test;
#[cfg(test)]
mod prepare_control_test;
#[cfg(test)]
mod prepare_ready_test;
#[cfg(test)]
mod test_support;

#[cfg(test)]
pub(super) use activation::{
    ClassicGroupFetchActivationErrorKind, ClassicGroupFetchActivationFailureKind,
    ClassicGroupFetchActivationFault, ClassicGroupFetchPostCoreFaultKind,
};
pub(super) use model::ClassicGroupFetchBuildError;
#[cfg(test)]
pub(super) use model::{
    ClassicGroupFetchCapturedFailure, ClassicGroupFetchFront, ClassicGroupFetchOwnerFaultKind,
    ClassicGroupFetchPreflightError,
};
pub(super) use owner::ClassicGroupFetchOwner;
