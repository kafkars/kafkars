//! Foreign follower state and driver-call mutation forbidden by this fixture.

fn intrude(owner: &mut Owner) {
    owner.apply_follower_join();
    owner.submit_one_classic_join();
    owner.settle_one_classic_join();
    owner.defer_join_leader();
    owner.stage_join_confirmation();
    owner.confirm_join();
    owner.join_call();
    owner.join_is_deferred();
    owner.try_reserve_join_group();
    owner.poll_join_group();
    owner.begin_join_group_settlement();
    owner.restore_join_group_settlement();
    owner.confirm_join_group_settlement();
    owner.submit_one_classic_sync();
    owner.settle_one_classic_sync();
    owner.prepared_sync();
    owner.begin_sync_handoff();
    owner.confirm_sync_driver_owned();
    owner.finish_sync_submission_failure();
    owner.sync_driver_owner();
    owner.stage_sync_confirmation();
    owner.confirm_sync();
    owner.try_reserve_sync_group();
    owner.poll_sync_group();
    owner.begin_sync_group_settlement();
    owner.restore_sync_group_settlement();
    owner.confirm_sync_group_settlement();
    owner.recover_classic_calls_after_driver_shutdown();
    owner.reconcile_join_after_driver_shutdown();
    owner.reconcile_sync_after_driver_shutdown();
    owner.inspect_sync_after_driver_shutdown();
    owner.has_entry_fault();
    owner.retained_owner_count();
    owner.retained_count();
}

struct Owner;

macro_rules! methods {
    ($($method:ident),* $(,)?) => {
        impl Owner {
            $(fn $method(&mut self) {})*
        }
    };
}

methods!(
    apply_follower_join,
    submit_one_classic_join,
    settle_one_classic_join,
    defer_join_leader,
    stage_join_confirmation,
    confirm_join,
    join_call,
    join_is_deferred,
    try_reserve_join_group,
    poll_join_group,
    begin_join_group_settlement,
    restore_join_group_settlement,
    confirm_join_group_settlement,
    submit_one_classic_sync,
    settle_one_classic_sync,
    prepared_sync,
    begin_sync_handoff,
    confirm_sync_driver_owned,
    finish_sync_submission_failure,
    sync_driver_owner,
    stage_sync_confirmation,
    confirm_sync,
    try_reserve_sync_group,
    poll_sync_group,
    begin_sync_group_settlement,
    restore_sync_group_settlement,
    confirm_sync_group_settlement,
    recover_classic_calls_after_driver_shutdown,
    reconcile_join_after_driver_shutdown,
    reconcile_sync_after_driver_shutdown,
    inspect_sync_after_driver_shutdown,
    has_entry_fault,
    retained_owner_count,
    retained_count,
);
