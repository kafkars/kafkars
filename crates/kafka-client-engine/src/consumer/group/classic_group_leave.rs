//! Explicit-close `LeaveGroup` ownership and one-at-a-time registry execution.

mod completion;
mod failure;
mod owner;
mod terminal;
mod turn;

pub(in crate::consumer) use completion::{
    GroupConsumerCloseAuthority, GroupConsumerCloseAuthorityClaim, GroupConsumerCloseCompletion,
    GroupConsumerCloseCompletionObservation, GroupConsumerCloseTerminal,
    GroupConsumerCloseTerminalFailure, GroupConsumerCloseTerminalFailureKind,
};
pub(in crate::consumer::group) use owner::ClassicGroupLeaveOwner;
pub(in crate::consumer::group) use turn::{
    ClassicGroupLeaveTurn, resolve_local_leave_without_member,
};

#[cfg(test)]
mod completion_test;
#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod owner_test;
