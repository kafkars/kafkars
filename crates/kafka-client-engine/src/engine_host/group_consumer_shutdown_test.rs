//! Group notifier fallback and post-driver recovery ordering scenarios.

use crate::consumer::GroupConsumerRegistry;

use super::group_consumer_shutdown::stop;

#[test]
fn failed_normal_stop_returns_the_exact_fallback_notifier_owner() {
    let mut registry =
        GroupConsumerRegistry::start().unwrap_or_else(|error| panic!("group registry: {error}"));

    let (stopped, fallback) = stop(&mut registry);

    assert!(stopped.is_err());
    let notifier = fallback.unwrap_or_else(|| panic!("fallback notifier owner"));
    notifier
        .join_off_notifier()
        .unwrap_or_else(|error| panic!("notifier join: {error}"));
}
