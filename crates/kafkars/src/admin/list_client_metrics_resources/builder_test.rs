//! Client-metrics resource builder surface tests.

use std::{future::Future, time::Duration};

use super::{
    ListClientMetricsResources, ListClientMetricsResourcesBuilder, ListClientMetricsResourcesResult,
};

fn assert_future<
    T: Future<Output = Result<ListClientMetricsResourcesResult, crate::KafkaError>>,
>() {
}

#[test]
fn operation_is_a_named_runtime_neutral_future() {
    assert_future::<ListClientMetricsResources>();
}

#[test]
fn builder_surface_keeps_timeout_configuration_inert() {
    let deadline: fn(
        ListClientMetricsResourcesBuilder,
        Duration,
    ) -> ListClientMetricsResourcesBuilder = ListClientMetricsResourcesBuilder::deadline_after;
    let submit: fn(ListClientMetricsResourcesBuilder) -> ListClientMetricsResources =
        ListClientMetricsResourcesBuilder::submit;

    let _ = (deadline, submit);
}
