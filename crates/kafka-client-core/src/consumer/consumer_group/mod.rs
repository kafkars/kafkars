//! Deterministic KIP-848 consumer-group heartbeat ownership.

mod apply;
mod effect;
mod error;
mod identity;
mod input;
mod leave;
mod load_retry;
mod machine;
mod model;
mod policy;
mod reconciliation;
mod reconciliation_stage;
mod reconciliation_success;
mod recovery;
mod rediscovery;
mod transition;
mod transition_support;

pub use effect::{ConsumerGroupHeartbeatEffect, ConsumerGroupHeartbeatTransition};
pub use error::{ConsumerGroupHeartbeatApplyError, ConsumerGroupHeartbeatErrorKind};
pub use identity::{
    ConsumerGroupHeartbeatAttempt, ConsumerGroupHeartbeatSchedule, ConsumerGroupHeartbeatSequence,
    ConsumerGroupMemberEpoch,
};
pub use input::ConsumerGroupHeartbeatInput;
pub use load_retry::ConsumerGroupHeartbeatRetrySchedule;
pub use machine::ConsumerGroupHeartbeatMachine;
pub use model::{
    CONSUMER_GROUP_HEARTBEAT_MAX_PARTITIONS, ConsumerGroupHeartbeatFailure,
    ConsumerGroupHeartbeatFatal, ConsumerGroupHeartbeatPhase, ConsumerGroupHeartbeatRequestKind,
};
pub use policy::{ConsumerGroupHeartbeatPolicy, ConsumerGroupHeartbeatPolicyError};

#[cfg(test)]
mod failure_test;
#[cfg(test)]
mod load_retry_identity_test;
#[cfg(test)]
mod load_retry_recovery_test;
#[cfg(test)]
mod load_retry_test;
#[cfg(test)]
mod load_retry_validation_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod reconciliation_terminal_test;
#[cfg(test)]
mod reconciliation_test;
#[cfg(test)]
mod recovery_test;
#[cfg(test)]
mod rediscovery_test;
#[cfg(test)]
mod test_support;
