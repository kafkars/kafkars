//! Stage-aware classic group policy for exact Kafka broker rejections.

use super::{ClassicBrokerError, ClassicCoordinatorRecovery};

const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const ILLEGAL_GENERATION: i16 = 22;
const UNKNOWN_MEMBER_ID: i16 = 25;
const REBALANCE_IN_PROGRESS: i16 = 27;

/// Kafka request stage that observed one broker rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassicBrokerStage {
    /// Dynamic `JoinGroup` versions one through three.
    Join,
    /// `SyncGroup` for the active membership cycle.
    Sync,
    /// Assignment-fenced `Heartbeat`.
    Heartbeat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassicErrorDisposition {
    Rejoin(ClassicCoordinatorRecovery),
    Fatal,
}

pub(super) const fn disposition(
    stage: ClassicBrokerStage,
    error: ClassicBrokerError,
) -> ClassicErrorDisposition {
    match (stage, error.code()) {
        (
            ClassicBrokerStage::Join,
            COORDINATOR_LOAD_IN_PROGRESS | UNKNOWN_MEMBER_ID | REBALANCE_IN_PROGRESS,
        )
        | (
            ClassicBrokerStage::Sync | ClassicBrokerStage::Heartbeat,
            ILLEGAL_GENERATION | UNKNOWN_MEMBER_ID | REBALANCE_IN_PROGRESS,
        ) => ClassicErrorDisposition::Rejoin(ClassicCoordinatorRecovery::Retain),
        (_, COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR) => {
            ClassicErrorDisposition::Rejoin(ClassicCoordinatorRecovery::Rediscover)
        }
        _ => ClassicErrorDisposition::Fatal,
    }
}
