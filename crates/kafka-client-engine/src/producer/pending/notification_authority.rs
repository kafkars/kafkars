//! Unforgeable right to run pending notification work off reactor threads.

use crate::completion::NotifierPendingDispatchOwner;

use super::backlog::PendingNotificationRecoveryDispatchOwner;

#[must_use = "dispatch authority must remain with its notifier or recovery owner"]
pub(crate) struct PendingNotificationDispatchAuthority {
    _dispatch_proof: (),
}

impl PendingNotificationDispatchAuthority {
    pub(crate) const fn from_notifier(_owner: NotifierPendingDispatchOwner) -> Self {
        Self {
            _dispatch_proof: (),
        }
    }

    pub(super) const fn from_recovery(_owner: PendingNotificationRecoveryDispatchOwner) -> Self {
        Self {
            _dispatch_proof: (),
        }
    }

    #[cfg(test)]
    pub(super) const fn from_test(
        _owner: super::test_support::PendingNotificationDispatchTestOwner,
    ) -> Self {
        Self {
            _dispatch_proof: (),
        }
    }
}
