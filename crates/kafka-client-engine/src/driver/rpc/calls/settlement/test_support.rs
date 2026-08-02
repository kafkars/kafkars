//! Test-only constructors and route-refresh controls for Produce settlement.

use kafka_client_core::{BatchExecutionId, Deadline, ProducerInput};
use kafka_driver::{RequestError, RouteFailureToken};

use super::{ProduceRouteRefresh, SettledProduceCall, TrackedProduceEntries};
use crate::driver::rpc::produce_call_entries::TrackedProduceEntry;

impl SettledProduceCall {
    pub(in crate::driver::rpc::calls) fn from_input(
        execution: BatchExecutionId,
        deadline: Deadline,
        input: ProducerInput,
        mut route_token: Option<RouteFailureToken>,
    ) -> Self {
        let route_refresh = ProduceRouteRefresh::from_input(input, &mut route_token);
        Self {
            entries: TrackedProduceEntries::Single(TrackedProduceEntry {
                execution,
                deadline,
                topic: "test".into(),
                partition: 0,
            }),
            result: Err(RequestError::RouteUnavailable),
            response_shape_valid: true,
            response_index: None,
            shared_deadline: deadline,
            input,
            _route_token: route_token,
            route_refresh,
        }
    }

    pub(in crate::driver::rpc::calls) fn with_submit_then_pending_refresh_for_test(
        execution: BatchExecutionId,
        deadline: Deadline,
        input: ProducerInput,
    ) -> Self {
        Self {
            entries: TrackedProduceEntries::Single(TrackedProduceEntry {
                execution,
                deadline,
                topic: "test".into(),
                partition: 0,
            }),
            result: Err(RequestError::RouteUnavailable),
            response_shape_valid: true,
            response_index: None,
            shared_deadline: deadline,
            input,
            _route_token: None,
            route_refresh: ProduceRouteRefresh::submit_for_test(),
        }
    }
}
