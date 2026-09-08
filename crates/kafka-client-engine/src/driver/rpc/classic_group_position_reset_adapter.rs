//! Declarative facade for original-deadline group reset routing and completion.

mod call;
#[cfg(test)]
mod call_test;

pub(crate) use call::{
    ClassicGroupPositionResetCall, ClassicGroupPositionResetCompletionError,
    ClassicGroupPositionResetOutcome, ClassicGroupPositionResetRoute,
};
