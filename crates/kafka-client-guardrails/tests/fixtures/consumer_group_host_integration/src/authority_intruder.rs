//! Forbidden construction and mutation of classic membership host authorities.

use crate::authority_owner::{
    ClassicGroupExecution, ClassicGroupJoinDriverAcceptance, ClassicGroupJoinHandoff,
    ClassicGroupJoinIntegrationOwner, ClassicGroupJoinTracking, GroupConsumerShardState,
    PreparedClassicGroupJoin,
};

fn intrude() {
    let mut prepared = PreparedClassicGroupJoin {
        prepared_join_identity: 1,
    };
    prepared.prepared_join_identity = 2;
    let mut handoff = ClassicGroupJoinHandoff { handed_off_join: 1 };
    handoff.handed_off_join = 2;
    let mut acceptance = ClassicGroupJoinDriverAcceptance { accepted_join: 1 };
    acceptance.accepted_join = 2;
    let mut tracking = ClassicGroupJoinTracking {
        tracked_join_identity: 1,
    };
    tracking.tracked_join_identity = 2;
    let mut driver_owned = ClassicGroupJoinIntegrationOwner {
        driver_owned_join: 1,
    };
    driver_owned.driver_owned_join = 2;
    let mut execution = ClassicGroupExecution {
        classic_execution_state: 1,
    };
    execution.classic_execution_state = 2;
    let mut shard = GroupConsumerShardState {
        registry_owner: 1,
        admission_fence: 1,
        reactor_wake: 1,
    };
    shard.admission_fence = 2;
}
