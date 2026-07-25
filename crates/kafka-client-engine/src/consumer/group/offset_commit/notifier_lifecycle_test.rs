//! `OffsetCommit` notifier identity, transfer, and drop-insurance scenarios.

use super::host::GroupOffsetCommitHost;

#[test]
fn fallback_transfer_preserves_join_ownership_after_unsettled_stop() {
    let mut host = GroupOffsetCommitHost::start_group_offset_commit_host()
        .unwrap_or_else(|error| panic!("host start: {error}"));
    assert!(host.notifier_thread_id().is_some());
    assert!(host.finish_shutdown().is_err());

    let join = host
        .take_notifier()
        .unwrap_or_else(|| panic!("fallback notifier owner"));
    assert!(host.take_notifier().is_none());
    join.join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}

#[test]
fn drop_closes_an_untransferred_notifier_without_becoming_join_owner() {
    let host = GroupOffsetCommitHost::start_group_offset_commit_host()
        .unwrap_or_else(|error| panic!("host start: {error}"));
    assert!(host.notifier_thread_id().is_some());
    drop(host);
}
