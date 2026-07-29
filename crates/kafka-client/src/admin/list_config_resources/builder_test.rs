//! Configuration-resource builder surface tests.

use std::{future::Future, time::Duration};

use super::{
    ConfigResourceType, ListConfigResources, ListConfigResourcesBuilder, ListConfigResourcesResult,
};

fn assert_future<T: Future<Output = Result<ListConfigResourcesResult, crate::KafkaError>>>() {}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<ListConfigResources>();
}

#[test]
fn builder_surface_keeps_filter_and_timeout_configuration_inert() {
    let resource_types: fn(
        ListConfigResourcesBuilder,
        Vec<ConfigResourceType>,
    ) -> ListConfigResourcesBuilder = ListConfigResourcesBuilder::resource_types;
    let deadline: fn(ListConfigResourcesBuilder, Duration) -> ListConfigResourcesBuilder =
        ListConfigResourcesBuilder::deadline_after;
    let submit: fn(ListConfigResourcesBuilder) -> ListConfigResources =
        ListConfigResourcesBuilder::submit;

    let _ = (resource_types, deadline, submit);
}
