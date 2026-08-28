//! Unforgeable ownership of one reserved exact-broker Produce call lane.

use kafka_driver::RoutedCall;
use kafka_wire::ProduceResponse;

use crate::clock::OperationDeadline;

use super::{super::produce_call_entries::TrackedProduceEntries, TrackedProduceCall};

#[must_use = "a reserved Produce call lane must be submitted or released"]
pub(crate) struct ProduceCallPermit<'a> {
    reserved_produce_calls: &'a mut Vec<TrackedProduceCall>,
    reserved_produce_broker_id: i32,
}

impl<'a> ProduceCallPermit<'a> {
    pub(super) fn from_reserved_exact_broker_lane(
        reserved_produce_calls: &'a mut Vec<TrackedProduceCall>,
        reserved_produce_broker_id: i32,
    ) -> Self {
        Self {
            reserved_produce_calls,
            reserved_produce_broker_id,
        }
    }

    pub(in crate::driver::rpc) const fn reserved_exact_broker_id(&self) -> i32 {
        self.reserved_produce_broker_id
    }

    pub(in crate::driver::rpc) fn commit_reserved_exact_broker_call(
        self,
        entries: TrackedProduceEntries,
        deadline: OperationDeadline,
        call: RoutedCall<ProduceResponse>,
    ) {
        self.reserved_produce_calls.push(TrackedProduceCall {
            entries,
            broker_id: self.reserved_produce_broker_id,
            deadline,
            call,
        });
    }
}
