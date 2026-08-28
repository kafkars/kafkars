//! Test-only constructors and route-refresh controls for Produce settlement.

use kafka_client_core::{BatchExecutionId, Moment, ProducerInput};
use kafka_driver::{RequestError, ResponseCloseReason, RouteFailureToken};

use super::{ProduceRouteRefresh, RecoveredProduceCall, SettledProduceCall, TrackedProduceEntries};
use crate::driver::rpc::calls::{TrackedProduceCall, TrackedProduceCalls};
use crate::{clock::OperationDeadline, driver::rpc::produce_call_entries::TrackedProduceEntry};

impl SettledProduceCall {
    pub(in crate::driver::rpc::calls) fn from_tracked_failure_for_test(
        call: TrackedProduceCall,
        now: Moment,
    ) -> Self {
        let TrackedProduceCall {
            entries,
            deadline,
            call,
            broker_id: _broker_id,
        } = call;
        drop(call);
        Self::from_terminal(
            entries,
            deadline,
            Err(RequestError::ConnectionClosed(
                ResponseCloseReason::TransportClosed,
            )),
            now,
            None,
        )
    }

    pub(in crate::driver::rpc::calls) fn from_input(
        execution: BatchExecutionId,
        deadline: OperationDeadline,
        input: ProducerInput,
        mut route_token: Option<RouteFailureToken>,
    ) -> Self {
        let route_refresh = ProduceRouteRefresh::from_input(input, &mut route_token);
        Self {
            entries: TrackedProduceEntries::Single(TrackedProduceEntry {
                execution,
                deadline: deadline.core(),
                topic: "test".into(),
                partition: 0,
            }),
            result: Err(RequestError::RouteUnavailable),
            response_shape_valid: true,
            response_index: None,
            deadline,
            input,
            _route_token: route_token,
            route_refresh,
        }
    }

    pub(in crate::driver::rpc::calls) fn with_submit_then_pending_refresh_for_test(
        execution: BatchExecutionId,
        deadline: OperationDeadline,
        input: ProducerInput,
    ) -> Self {
        Self {
            entries: TrackedProduceEntries::Single(TrackedProduceEntry {
                execution,
                deadline: deadline.core(),
                topic: "test".into(),
                partition: 0,
            }),
            result: Err(RequestError::RouteUnavailable),
            response_shape_valid: true,
            response_index: None,
            deadline,
            input,
            _route_token: None,
            route_refresh: ProduceRouteRefresh::submit_for_test(),
        }
    }

    pub(in crate::driver::rpc::calls) fn complete_route_refresh_for_test(&mut self) {
        self.route_refresh = ProduceRouteRefresh::Refreshed;
        super::mark_route_refreshed(&mut self.input);
    }

    pub(in crate::driver::rpc) const fn operation_deadline_for_test(&self) -> OperationDeadline {
        self.deadline
    }
}

impl TrackedProduceCalls {
    pub(crate) fn new(capacity: usize) -> Self {
        Self::with_max_in_flight_requests_per_broker(capacity, 5)
    }

    pub(crate) fn recovered(&self) -> &[RecoveredProduceCall] {
        &self.recovered
    }
}
