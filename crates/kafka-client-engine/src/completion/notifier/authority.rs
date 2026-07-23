//! Private proof that only the notifier owner may mint dispatch authority.

#[must_use = "convert notifier ownership into one scoped dispatch authority"]
pub(crate) struct NotifierPendingDispatchOwner {
    _notifier_owner_proof: (),
}

impl NotifierPendingDispatchOwner {
    pub(super) const fn new() -> Self {
        Self {
            _notifier_owner_proof: (),
        }
    }
}
