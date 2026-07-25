//! Fixed-capacity classic transition ordering evidence.

use crate::{Deadline, GroupId};

use super::{
    ClassicGroupEffect, ClassicGroupTiming, ClassicGroupTransition, ClassicProtocol,
    MembershipCycle,
};

#[test]
fn transition_owns_one_effect_without_a_dynamic_effect_queue() {
    let group_id = GroupId::try_from_raw(1).unwrap_or_else(|| panic!("nonzero group"));
    let cycle = MembershipCycle::initial();
    let first = ClassicGroupEffect::Join {
        group_id,
        cycle,
        protocol: ClassicProtocol::Range,
        timing: ClassicGroupTiming::try_new(10_000, 30_000)
            .unwrap_or_else(|error| panic!("valid timing: {error}")),
        deadline: Deadline::from_tick(9),
    };
    let effects = ClassicGroupTransition::one(first)
        .into_effects()
        .collect::<Vec<_>>();
    assert_eq!(effects.len(), 1);
}
