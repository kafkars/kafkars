//! Declarative boundary for deterministic Fetch activation and settlement state.

mod activation;
mod settlement;
mod transition;

pub(super) use activation::RetainedFetchActivation;
