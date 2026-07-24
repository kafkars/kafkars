//! Provisional group-consumer checkpoint identity pending commit execution.

/// Assignment-fenced next offsets for processed group-consumer records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub(super) group_id: String,
    pub(super) assignment_epoch: u64,
}

impl Checkpoint {
    /// Returns the assignment generation used to reject stale commits.
    pub const fn assignment_epoch(&self) -> u64 {
        self.assignment_epoch
    }
}
