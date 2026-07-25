//! Validated driver-owned routing for one classic consumer-group spelling.

use kafka_driver::{CoordinatorKey, CoordinatorKeyError, CoordinatorKind, Route};

pub(super) fn group_coordinator_route(group: &str) -> Result<Route, CoordinatorKeyError> {
    let key = CoordinatorKey::new(CoordinatorKind::Group, group)?;
    Ok(Route::Coordinator { key })
}
