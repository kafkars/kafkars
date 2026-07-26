//! Declarative facade for assignment-fenced classic-group Fetch activation ownership.

mod activation;
mod owner;

#[cfg(test)]
mod activation_test;
#[cfg(test)]
mod owner_test;
#[cfg(test)]
mod test_support;

#[cfg(test)]
pub(super) use activation::{
    ClassicGroupFetchActivationErrorKind, ClassicGroupFetchActivationFailureKind,
    ClassicGroupFetchActivationFault, ClassicGroupFetchPostCoreFaultKind,
};
pub(super) use owner::ClassicGroupFetchOwner;
