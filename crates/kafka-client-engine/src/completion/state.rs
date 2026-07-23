//! Mutex-protected observer state owned only by observers and the notifier.

use std::task::Waker;

use super::CompletionId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Presence {
    Active,
    Abandoned,
}

pub(super) enum CellPhase<T> {
    Vacant {
        generation: u64,
    },
    Pending {
        id: CompletionId,
        presence: Presence,
        waker: Option<Waker>,
    },
    Terminal {
        id: CompletionId,
        presence: Presence,
        value: Option<T>,
    },
    ReclaimPending {
        id: CompletionId,
    },
    ReclaimQueued {
        id: CompletionId,
    },
    Retired,
}

pub(super) fn take_terminal<T>(phase: &mut CellPhase<T>) -> Option<T> {
    let CellPhase::Terminal { value, .. } = phase else {
        return None;
    };
    value.take()
}
