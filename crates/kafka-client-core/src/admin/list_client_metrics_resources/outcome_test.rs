//! Exact API-74 result and broker-rejection value scenarios.

use core::num::NonZeroI16;

use super::{ListClientMetricsResourcesBrokerError, ListClientMetricsResourcesListing};

#[test]
fn listing_preserves_throttle_and_canonical_names() {
    let listing =
        ListClientMetricsResourcesListing::new(27, vec!["alpha".to_owned(), "zeta".to_owned()]);
    assert_eq!(listing.throttle_time_ms(), 27);
    assert_eq!(listing.resource_names(), ["alpha", "zeta"]);
    assert_eq!(
        listing.into_parts(),
        (27, vec!["alpha".to_owned(), "zeta".to_owned()])
    );
}

#[test]
fn broker_error_preserves_throttle_and_negative_unknown_code() {
    let error = ListClientMetricsResourcesBrokerError::new(
        13,
        NonZeroI16::new(-32_000).unwrap_or_else(|| panic!("nonzero code")),
    );
    assert_eq!(error.throttle_time_ms(), 13);
    assert_eq!(error.code(), -32_000);
    assert_eq!(error.into_parts(), (13, -32_000));
}
