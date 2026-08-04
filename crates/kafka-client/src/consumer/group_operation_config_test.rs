//! Public hosted-group operation defaults and replacement evidence.

use std::time::Duration;

use super::GroupConsumerOperationConfig;

#[test]
fn defaults_and_independent_replacements_are_exact() {
    let default = GroupConsumerOperationConfig::default();
    assert_eq!(default.seek_timeout(), Duration::from_secs(30));
    assert_eq!(default.close_timeout(), Duration::from_secs(30));

    let configured = default
        .with_seek_timeout(Duration::from_secs(11))
        .with_close_timeout(Duration::from_secs(17));
    assert_eq!(configured.seek_timeout(), Duration::from_secs(11));
    assert_eq!(configured.close_timeout(), Duration::from_secs(17));
}
