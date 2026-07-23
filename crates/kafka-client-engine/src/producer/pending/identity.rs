//! Generation-fenced identities for records waiting before core admission.

/// Exact identity of one live pending-admission slot generation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct PendingAdmissionId {
    slot: usize,
    generation: u64,
}

impl PendingAdmissionId {
    pub(super) const fn new(slot: usize, generation: u64) -> Self {
        Self { slot, generation }
    }

    pub(super) const fn slot(self) -> usize {
        self.slot
    }

    pub(super) const fn generation(self) -> u64 {
        self.generation
    }
}
