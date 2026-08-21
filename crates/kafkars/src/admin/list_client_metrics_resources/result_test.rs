//! Stable generated-free client-metrics resource result tests.

use std::time::Duration;

use super::ListClientMetricsResourcesResult;

#[test]
fn result_preserves_throttle_and_canonical_resource_order() {
    let result = ListClientMetricsResourcesResult::new(
        Duration::from_millis(19),
        vec!["alpha".to_owned(), "zeta".to_owned()],
    );

    assert_eq!(result.throttle(), Duration::from_millis(19));
    assert_eq!(result.resources(), ["alpha", "zeta"]);
    assert_eq!(
        result.into_parts(),
        (
            Duration::from_millis(19),
            vec!["alpha".to_owned(), "zeta".to_owned()]
        )
    );
}
