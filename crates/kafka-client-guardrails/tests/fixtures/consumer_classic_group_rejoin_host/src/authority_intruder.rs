//! Deliberate foreign construction and mutation of classic rejoin authorities.

use crate::authority_owner::{
    ClassicGroupRejoinExecution, ClassicRejectionPostCore, ClassicRejoinPostCore,
    PendingClassicRejoinJoin,
};

fn steal() {
    let mut execution = ClassicGroupRejoinExecution {
        rejoin_execution_state: 1,
    };
    execution.rejoin_execution_state = 2;
    let mut join = PendingClassicRejoinJoin {
        pending_rejoin_group_id: 1,
        pending_rejoin_cycle: 1,
        pending_rejoin_protocol: 1,
        pending_rejoin_timing: 1,
        pending_rejoin_deadline: 1,
    };
    join.pending_rejoin_cycle = 2;
    let mut fault = ClassicRejoinPostCore {
        post_core_rejoin_join: 1,
        post_core_rejoin_other: 1,
        post_core_rejoin_failure: 1,
    };
    fault.post_core_rejoin_failure = 2;
    let mut rejection = ClassicRejectionPostCore {
        post_core_rejection_effects: 1,
        post_core_rejection_failure: 1,
    };
    rejection.post_core_rejection_failure = 2;
}
