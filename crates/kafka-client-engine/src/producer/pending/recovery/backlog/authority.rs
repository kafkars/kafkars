//! Private proof that only recovery ownership may mint dispatch authority.

#[must_use = "convert recovery ownership into one scoped dispatch authority"]
pub(crate) struct PendingNotificationRecoveryDispatchOwner {
    _recovery_owner_proof: (),
}

impl PendingNotificationRecoveryDispatchOwner {
    pub(super) const fn new() -> Self {
        Self {
            _recovery_owner_proof: (),
        }
    }
}
