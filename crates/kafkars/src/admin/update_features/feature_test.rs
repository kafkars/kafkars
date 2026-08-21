//! Explicit finalized-feature intent tests.

use super::{FeatureUpdate, FeatureUpdateIntent};

#[test]
fn every_change_direction_requires_a_named_constructor() {
    let upgrade = FeatureUpdate::upgrade("metadata.version", 12);
    let safe = FeatureUpdate::safe_downgrade("transaction.version", 4);
    let unsafe_change = FeatureUpdate::unsafe_downgrade("group.version", 0);

    assert_eq!(upgrade.feature_name(), "metadata.version");
    assert_eq!(upgrade.max_version_level(), 12);
    assert_eq!(upgrade.intent(), FeatureUpdateIntent::Upgrade);
    assert!(!upgrade.is_deletion());

    assert_eq!(safe.intent(), FeatureUpdateIntent::SafeDowngrade);
    assert!(!safe.is_deletion());

    assert_eq!(unsafe_change.intent(), FeatureUpdateIntent::UnsafeDowngrade);
    assert!(unsafe_change.is_deletion());
}

#[test]
fn consuming_an_update_preserves_exact_intent_and_scalars() {
    let update = FeatureUpdate::unsafe_downgrade("metadata.version", 7);
    assert_eq!(
        update.into_parts(),
        (
            String::from("metadata.version"),
            7,
            FeatureUpdateIntent::UnsafeDowngrade,
        )
    );
}
