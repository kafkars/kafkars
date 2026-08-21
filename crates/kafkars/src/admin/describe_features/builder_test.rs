//! Kafka feature discovery builder surface tests.

use std::time::Duration;

use super::{DescribeFeatures, DescribeFeaturesBuilder};

#[test]
fn builder_keeps_deadline_configuration_inert_until_submit() {
    let deadline_after: fn(DescribeFeaturesBuilder, Duration) -> DescribeFeaturesBuilder =
        DescribeFeaturesBuilder::deadline_after;
    let submit: fn(DescribeFeaturesBuilder) -> DescribeFeatures = DescribeFeaturesBuilder::submit;

    let _ = (deadline_after, submit);
}
