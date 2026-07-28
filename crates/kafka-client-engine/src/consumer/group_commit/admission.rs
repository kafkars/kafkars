//! Capture-first public admission of one exact linear group checkpoint.

use std::{fmt, sync::Arc, time::Duration};

use super::GroupConsumerCommitObserver;
use crate::consumer::{
    GroupConsumerCheckpoint, GroupConsumerHandle, group::GroupConsumerCommitPortErrorCategory,
};

impl GroupConsumerHandle {
    /// Attempts one bounded assignment-fenced offset commit.
    ///
    /// The timeout is captured before registry contention or checkpoint
    /// validation. Rejection returns the exact non-clone checkpoint. Once
    /// accepted, wake and internal host faults are advisory and the returned
    /// observer remains the sole terminal authority.
    pub fn try_commit(
        &mut self,
        checkpoint: GroupConsumerCheckpoint,
        timeout: Duration,
    ) -> Result<GroupConsumerCommitAccepted, GroupConsumerCommitAdmissionError> {
        let capture = match self.port.capture_commit_deadline(timeout) {
            Ok(capture) => capture,
            Err(_error) => {
                return Err(GroupConsumerCommitAdmissionError {
                    kind: GroupConsumerCommitAdmissionErrorKind::InvalidDeadline,
                    checkpoint,
                });
            }
        };
        let (observation, submission) = match checkpoint.try_into_commit_parts() {
            Ok(parts) => parts,
            Err(checkpoint) => {
                return Err(GroupConsumerCommitAdmissionError {
                    kind: GroupConsumerCommitAdmissionErrorKind::Backpressure,
                    checkpoint,
                });
            }
        };
        let admission = match self
            .port
            .admit_captured_commit(self.group_id, capture, submission)
        {
            Ok(admission) => admission,
            Err(failure) => {
                let kind = admission_error_kind(failure.public_category());
                drop(failure.into_checkpoint());
                let checkpoint = observation.into_checkpoint();
                return Err(GroupConsumerCommitAdmissionError { kind, checkpoint });
            }
        };
        let (accepted, wake_failed) = admission.into_public_parts();
        let host_faulted = accepted.host_faulted();
        Ok(GroupConsumerCommitAccepted {
            observer: GroupConsumerCommitObserver::new(
                accepted.into_observer(),
                observation,
                Arc::clone(&self.lifetime),
            ),
            host_faulted,
            wake_failed,
        })
    }
}

/// Stable reason a group checkpoint did not enter terminal ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerCommitAdmissionErrorKind {
    /// The requested timeout could not form an absolute deadline.
    InvalidDeadline,
    /// Engine or group commit admission has closed.
    Closed,
    /// Another owner currently holds the bounded group registry.
    Contended,
    /// The registered group is closing or no longer present.
    GroupUnavailable,
    /// A fixed count, byte, result, or allocation budget is full.
    Backpressure,
    /// The checkpoint no longer matches the live member assignment.
    StaleCheckpoint,
    /// An accepted internal owner fault prevents new commit admission.
    HostUnavailable,
}

/// Pre-admission rejection retaining the exact caller checkpoint.
#[must_use = "commit rejection retains the exact non-clone checkpoint"]
pub struct GroupConsumerCommitAdmissionError {
    kind: GroupConsumerCommitAdmissionErrorKind,
    checkpoint: GroupConsumerCheckpoint,
}

impl GroupConsumerCommitAdmissionError {
    /// Returns the stable pre-admission rejection category.
    pub const fn kind(&self) -> GroupConsumerCommitAdmissionErrorKind {
        self.kind
    }

    /// Recovers the exact checkpoint that did not transfer.
    pub fn into_checkpoint(self) -> GroupConsumerCheckpoint {
        self.checkpoint
    }
}

impl fmt::Debug for GroupConsumerCommitAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GroupConsumerCommitAdmissionError")
            .field("kind", &self.kind)
            .field("checkpoint", &self.checkpoint)
            .finish()
    }
}

impl fmt::Display for GroupConsumerCommitAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "classic-group checkpoint commit rejected: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for GroupConsumerCommitAdmissionError {}

/// Accepted commit ownership plus advisory post-admission diagnostics.
#[must_use = "accepted commit retains its sole terminal observer"]
pub struct GroupConsumerCommitAccepted {
    observer: GroupConsumerCommitObserver,
    host_faulted: bool,
    wake_failed: bool,
}

impl GroupConsumerCommitAccepted {
    /// Reports that accepted work exposed a retained internal owner fault.
    pub const fn host_faulted(&self) -> bool {
        self.host_faulted
    }

    /// Reports that the advisory reactor wake failed after admission.
    pub const fn wake_failed(&self) -> bool {
        self.wake_failed
    }

    /// Transfers the sole terminal observer.
    pub fn into_observer(self) -> GroupConsumerCommitObserver {
        self.observer
    }
}

impl fmt::Debug for GroupConsumerCommitAccepted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GroupConsumerCommitAccepted")
            .field("observer", &self.observer)
            .field("host_faulted", &self.host_faulted)
            .field("wake_failed", &self.wake_failed)
            .finish()
    }
}

const fn admission_error_kind(
    category: GroupConsumerCommitPortErrorCategory,
) -> GroupConsumerCommitAdmissionErrorKind {
    match category {
        GroupConsumerCommitPortErrorCategory::InvalidDeadline => {
            GroupConsumerCommitAdmissionErrorKind::InvalidDeadline
        }
        GroupConsumerCommitPortErrorCategory::Closed => {
            GroupConsumerCommitAdmissionErrorKind::Closed
        }
        GroupConsumerCommitPortErrorCategory::Contended => {
            GroupConsumerCommitAdmissionErrorKind::Contended
        }
        GroupConsumerCommitPortErrorCategory::GroupUnavailable => {
            GroupConsumerCommitAdmissionErrorKind::GroupUnavailable
        }
        GroupConsumerCommitPortErrorCategory::Backpressure => {
            GroupConsumerCommitAdmissionErrorKind::Backpressure
        }
        GroupConsumerCommitPortErrorCategory::StaleCheckpoint => {
            GroupConsumerCommitAdmissionErrorKind::StaleCheckpoint
        }
        GroupConsumerCommitPortErrorCategory::HostUnavailable => {
            GroupConsumerCommitAdmissionErrorKind::HostUnavailable
        }
    }
}
