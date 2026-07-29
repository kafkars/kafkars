//! Inert finalized-feature update builder surface tests.

use std::time::Duration;

use super::{UpdateFeatures, UpdateFeaturesBuilder};

#[test]
fn builder_names_validation_and_deadline_controls() {
    let validate_only: fn(UpdateFeaturesBuilder, bool) -> UpdateFeaturesBuilder =
        UpdateFeaturesBuilder::validate_only;
    let deadline_after: fn(UpdateFeaturesBuilder, Duration) -> UpdateFeaturesBuilder =
        UpdateFeaturesBuilder::deadline_after;
    let submit: fn(UpdateFeaturesBuilder) -> UpdateFeatures = UpdateFeaturesBuilder::submit;

    let _ = (validate_only, deadline_after, submit);
}
