//! Same-broker routed Fetch collection without plan or driver ownership.

use std::sync::Arc;

use kafka_client_core::{AssignedConsumerMachine, FetchOwnership, Moment};

use crate::driver::BrokerId;

use super::{
    broker_session::BrokerSessionMember, executor::DirectFetchExecutor,
    prepared::PreparedFetchExecution,
};

impl DirectFetchExecutor {
    pub(super) fn collect_same_broker_routed(
        &mut self,
        broker_id: BrokerId,
        machine: &AssignedConsumerMachine,
        now: Moment,
        prepared_batch: &mut Vec<PreparedFetchExecution>,
    ) {
        let mut selected_bytes = prepared_batch
            .iter()
            .try_fold(0usize, |bytes, prepared| {
                bytes.checked_add(prepared.hard_output_bytes)
            })
            .unwrap_or(usize::MAX);
        let mut index = self.routed.len();
        while index > 0 {
            index -= 1;
            if self.routed[index].broker_id != broker_id {
                continue;
            }
            let fence = self.routed[index].request.fence();
            match machine.fetch_ownership(fence) {
                Ok(FetchOwnership::Superseded) => {
                    self.routed.swap_remove(index);
                }
                Ok(FetchOwnership::Active)
                    if !self.routed[index]
                        .request
                        .operation_deadline()
                        .core()
                        .is_elapsed_at(now) =>
                {
                    let Some(next_bytes) =
                        selected_bytes.checked_add(self.routed[index].hard_output_bytes)
                    else {
                        continue;
                    };
                    if !self.can_reserve_broker_batch(
                        prepared_batch.len().saturating_add(1),
                        next_bytes,
                    ) {
                        continue;
                    }
                    let routed = self.routed.swap_remove(index);
                    selected_bytes = next_bytes;
                    prepared_batch.push(PreparedFetchExecution::from_parts(
                        routed.request,
                        routed.hard_output_bytes,
                    ));
                }
                Ok(FetchOwnership::Active) | Err(_) => {}
            }
        }
    }
}

pub(super) fn broker_session_members(
    prepared: &[PreparedFetchExecution],
) -> Vec<BrokerSessionMember> {
    prepared
        .iter()
        .map(|prepared| {
            BrokerSessionMember::new(
                prepared.fence().position(),
                Arc::from(prepared.request.topic()),
            )
        })
        .collect()
}
