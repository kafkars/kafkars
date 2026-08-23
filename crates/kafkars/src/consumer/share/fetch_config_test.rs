//! Public `ShareFetch` configuration defaults and replacement evidence.

use std::time::Duration;

use super::ShareConsumerFetchConfig;

#[test]
fn defaults_and_replacements_are_explicit() {
    let defaults = ShareConsumerFetchConfig::default();
    assert_eq!(defaults.max_wait(), Duration::from_millis(500));
    assert_eq!(defaults.min_bytes(), 1);
    assert_eq!(defaults.max_bytes(), 1024 * 1024);
    assert_eq!(defaults.max_records(), 500);
    assert_eq!(defaults.batch_size(), 500);
    assert_eq!(defaults.attempt_timeout(), Duration::from_secs(30));

    let selected = defaults
        .with_max_wait(Duration::from_millis(250))
        .with_min_bytes(2)
        .with_max_bytes(4096)
        .with_max_records(32)
        .with_batch_size(8)
        .with_attempt_timeout(Duration::from_secs(7));
    assert_eq!(
        selected.into_parts(),
        (
            Duration::from_millis(250),
            2,
            4096,
            32,
            8,
            Duration::from_secs(7)
        )
    );
}
