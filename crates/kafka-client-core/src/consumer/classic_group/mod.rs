//! Deterministic classic consumer-group Join and Sync policy.

mod apply;
mod assignment;
mod effect;
mod error;
mod identity;
mod input;
mod machine;
mod model;
mod range_validation;
mod terminal_transition;
mod transition;
mod transition_support;

pub use assignment::{ClassicAssignmentError, ClassicAssignmentPlan, ClassicMemberAssignment};
pub use effect::{ClassicGroupEffect, ClassicGroupTransition};
pub use error::{ClassicGroupApplyError, ClassicGroupErrorKind};
pub use identity::{ClassicGeneration, JoinedMemberSlot, MemberRank, MembershipCycle};
pub use input::ClassicGroupInput;
pub use machine::ClassicGroupMachine;
pub use model::{
    ClassicGroupPhase, ClassicJoinMember, ClassicJoinMembers, ClassicJoinMembersError,
    ClassicProtocol, ClassicSubscription, ClassicSubscriptionError, TopicPartitionCount,
};

#[cfg(test)]
mod apply_test;
#[cfg(test)]
mod assignment_test;
#[cfg(test)]
mod effect_test;
#[cfg(test)]
mod error_test;
#[cfg(test)]
mod identity_test;
#[cfg(test)]
mod input_test;
#[cfg(test)]
mod leader_fencing_test;
#[cfg(test)]
mod machine_test;
#[cfg(test)]
mod model_test;
#[cfg(test)]
mod range_validation_test;
#[cfg(test)]
mod terminal_transition_test;
#[cfg(test)]
mod transition_support_test;
#[cfg(test)]
mod transition_test;
