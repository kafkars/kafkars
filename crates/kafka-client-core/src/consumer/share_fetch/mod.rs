//! Deterministic KIP-932 `ShareFetch` session and acquisition ownership.

mod acquisition;
mod identity;
mod ledger;
mod ledger_retirement;
mod model;
mod policy;
mod session;
mod session_error;

pub use acquisition::{ShareAcquisition, ShareAcquisitionPhase, ShareAcquisitionRelease};
pub use identity::{
    ShareAcquisitionGeneration, ShareFetchAssignmentGeneration, ShareFetchAttempt,
    ShareFetchBrokerId, ShareFetchSessionEpoch, ShareFetchSessionFence,
};
pub use ledger::ShareAcquisitionLedger;
pub use model::{
    ShareAcquiredOffsets, ShareAcquiredRange, ShareAcquiredRangeError, ShareDeliveryCount,
    ShareTopicUuid,
};
pub use policy::{
    ShareAcquisitionAdmissionError, ShareAcquisitionAdmissionErrorKind, ShareAcquisitionPolicy,
    ShareAcquisitionPolicyError,
};
pub use session::{
    SHARE_FETCH_MAX_PARTITIONS_PER_BROKER, ShareFetchSessionMachine, ShareFetchSessionPhase,
};
pub use session_error::{
    ShareFetchSessionApplyError, ShareFetchSessionErrorKind, ShareFetchSessionOpenError,
    ShareFetchSettlementError, ShareFetchSettlementErrorKind,
};

#[cfg(test)]
mod ledger_retirement_test;
#[cfg(test)]
mod ledger_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod session_test;
#[cfg(test)]
mod test_support;
