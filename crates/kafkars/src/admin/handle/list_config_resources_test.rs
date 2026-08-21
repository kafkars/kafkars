//! Admin configuration-resource listing entry-point surface tests.

use super::Admin;
use crate::admin::ListConfigResourcesBuilder;

#[test]
fn configuration_resource_listing_starts_as_an_inert_builder() {
    let method: fn(&Admin) -> ListConfigResourcesBuilder = Admin::list_config_resources;

    let _ = method;
}
