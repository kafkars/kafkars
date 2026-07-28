//! Same-cycle static replacement Join prepared from one broker-required member spelling.

use std::sync::Arc;

use kafka_client_core::{ClassicGroupEffect, ClassicGroupInput, MembershipCycle, Moment};

use crate::{
    consumer::group::{
        classic_group_join::PreparedClassicGroupJoin, registry_entry::GroupConsumerEntry,
    },
    driver::classic_group::JoinGroupTerminal,
};

use super::{ClassicGroupExecutionError, JoinInterpretationFailure, post_core, restore};

#[expect(
    clippy::result_large_err,
    reason = "post-core rejection retains the exact generated terminal and recovery effects"
)]
pub(super) fn prepare_member_id_required_join(
    entry: &mut GroupConsumerEntry,
    cycle: MembershipCycle,
    now: Moment,
    terminal: &JoinGroupTerminal,
    member: Arc<str>,
) -> Result<PreparedClassicGroupJoin, JoinInterpretationFailure> {
    if !entry.is_active() || entry.catalog.group_instance_id().is_none() {
        return Err(restore(ClassicGroupExecutionError::JoinTerminal));
    }
    let required = entry
        .catalog
        .prepare_required_join_member(cycle, member)
        .map_err(|_error| restore(ClassicGroupExecutionError::JoinTerminal))?;
    let member_id = required.member_id;
    let transition = entry
        .classic
        .apply(ClassicGroupInput::JoinMemberIdRequired {
            cycle,
            now,
            assigned_member_id: Some(member_id),
        })
        .map_err(|error| restore(ClassicGroupExecutionError::Core(error.kind())))?;
    entry.catalog.commit_required_join_member(required);
    let mut effects = transition.into_effects();
    let Some(ClassicGroupEffect::Join {
        group_id,
        cycle: replacement_cycle,
        protocol,
        member_id: Some(replacement_member_id),
        timing,
        deadline,
    }) = effects.next()
    else {
        return Err(post_core(ClassicGroupExecutionError::JoinTerminal));
    };
    if effects.next().is_some()
        || group_id != entry.group_id()
        || replacement_cycle != cycle
        || replacement_member_id != member_id
        || timing != entry.classic.machine().timing()
        || deadline != terminal.key().deadline().core()
    {
        return Err(post_core(ClassicGroupExecutionError::JoinTerminal));
    }
    Ok(PreparedClassicGroupJoin::new_with_member_id(
        group_id,
        cycle,
        protocol,
        Some(member_id),
        timing,
        terminal.key().deadline(),
    ))
}
