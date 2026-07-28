//! Stable diagnostics for producer host invariant failures.

use std::fmt;

use super::ProducerHostInvariantError;

impl fmt::Display for ProducerHostInvariantError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Core(error) => write!(formatter, "core transition invariant failed: {error}"),
            Self::Store(error) => write!(formatter, "producer store invariant failed: {error}"),
            Self::Binding(error) => write!(
                formatter,
                "producer completion binding invariant failed: {error}"
            ),
            Self::FlushBinding(error) => write!(
                formatter,
                "producer flush completion binding invariant failed: {error}"
            ),
            Self::Timer(error) => write!(formatter, "producer timer invariant failed: {error}"),
            Self::Completion(error) => {
                write!(formatter, "producer completion invariant failed: {error}")
            }
            Self::Reclaim(error) => write!(
                formatter,
                "producer completion reclaim invariant failed: {error}"
            ),
            Self::Prepared(error) => {
                write!(formatter, "prepared producer execution failed: {error}")
            }
            Self::Compression(error) => {
                write!(
                    formatter,
                    "producer compression invariant failed: {error:?}"
                )
            }
            Self::Revision(error) => {
                write!(formatter, "producer execution revision failed: {error}")
            }
            Self::MissingAdmissionIdentity => {
                formatter.write_str("accepted producer transition omitted its operation identity")
            }
            Self::MissingCancellationOutcome => {
                formatter.write_str("producer cancellation omitted its core-owned outcome")
            }
            Self::UnexpectedCancellationEffect => {
                formatter.write_str("producer cancellation emitted a time-dependent effect")
            }
            Self::CommittedFactsMismatch => {
                formatter.write_str("committed producer record facts changed after core admission")
            }
            Self::GeneratedFactCapacity => {
                formatter.write_str("producer generated-fact queue exceeded its fixed capacity")
            }
            Self::PendingEffectCapacity => {
                formatter.write_str("producer pending-effect storage exceeded its fixed capacity")
            }
            Self::TerminalBacklogCapacity => {
                formatter.write_str("producer terminal backlog exceeded completion-slot capacity")
            }
            Self::MissingFlushIdentity => {
                formatter.write_str("accepted producer flush omitted its flush identity")
            }
            Self::UnexpectedDriverInput => {
                formatter.write_str("producer driver bridge received a non-driver input")
            }
            Self::WaitingOwnership => {
                formatter.write_str("producer waiting policy and byte ownership diverged")
            }
            Self::WaitingToken => {
                formatter.write_str("producer waiting admission token was poisoned")
            }
            #[cfg(test)]
            Self::ForcedTerminalInterpretation => {
                formatter.write_str("forced terminal producer interpretation failure")
            }
            #[cfg(test)]
            Self::ForcedTerminalPlanning => {
                formatter.write_str("forced terminal producer planning failure")
            }
        }
    }
}
