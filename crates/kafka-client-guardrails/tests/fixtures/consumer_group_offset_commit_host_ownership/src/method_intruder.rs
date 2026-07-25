//! Deliberately steals group offset-commit execution and shutdown methods.

fn steal<T>(owner: &mut T, driver: &T) {
    owner.try_reserve_group_commit();
    owner.poll_group_commit();
    owner.begin_group_commit_settlement();
    owner.confirm_group_commit_settlement();
    owner.restore_group_commit_settlement();
    owner.recover_group_commits_after_driver_shutdown();
    owner.submit_prebuilt(driver);
    owner.pop_active();
    owner.take_settled();
    owner.pending_operation_id();
    owner.clear_pending_operation_id();
    owner.take_completion();
    owner.into_generated_offset_commit_request();
    owner.settle_preparation_failure();
    owner.retain_preparation_fault();
    owner.replay_recovered_settlements();
    owner.recover_pending_confirmation();
    owner.settle_transport_owned_failure();
    owner.replace_attempt();
    owner.replace_terminal();
}
