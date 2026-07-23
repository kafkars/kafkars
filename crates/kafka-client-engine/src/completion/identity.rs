//! Generation-fenced identities for fixed completion slots.

/// Identity of one reserved engine completion slot.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CompletionId {
    slot: usize,
    generation: u64,
}

impl CompletionId {
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
