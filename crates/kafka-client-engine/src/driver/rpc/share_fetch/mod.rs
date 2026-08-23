//! Exact-broker tracked-call boundary for `ShareFetch` v1.
#![allow(
    dead_code,
    reason = "the tracked-call checkpoint precedes hosted broker-session ownership"
)]

mod call;
#[cfg(test)]
mod call_test;
mod failure;
#[cfg(test)]
mod failure_test;
mod route;
mod submission;
#[cfg(test)]
mod submission_test;
mod terminal;
#[cfg(test)]
mod terminal_test;

#[expect(
    unused_imports,
    reason = "hosted ShareFetch call settlement lands in the next checkpoint"
)]
pub(crate) use call::{
    ShareFetchCall, ShareFetchCallEvidence, ShareFetchCompletionErrorKind,
    ShareFetchCompletionFailure, ShareFetchDriverSubmitFailure, ShareFetchRecoveredCall,
};
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "hosted ShareFetch failure settlement lands in the next checkpoint"
    )
)]
pub(crate) use failure::ShareFetchDriverFailureKind;
#[expect(
    unused_imports,
    reason = "hosted ShareFetch route settlement lands in the next checkpoint"
)]
pub(crate) use route::ShareFetchRoute;
#[expect(
    unused_imports,
    reason = "hosted ShareFetch admission mapping lands in the next checkpoint"
)]
pub(crate) use submission::ShareFetchDriverSubmitErrorKind;
#[expect(
    unused_imports,
    reason = "hosted ShareFetch terminal settlement lands in the next checkpoint"
)]
pub(crate) use terminal::{ShareFetchRawTerminal, ShareFetchResolution, ShareFetchTerminalContext};
