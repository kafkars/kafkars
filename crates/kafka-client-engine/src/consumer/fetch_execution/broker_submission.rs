//! Atomic output reservation and admission for one aggregate broker Fetch plan.

use kafka_client_core::{
    AssignedConsumerMachine, AssignedConsumerTransition, FetchFailure, Moment,
};

use crate::{
    driver::{
        BrokerFetchCallAdmission, BrokerId, DriverOwner, PartitionFetchRequest,
        classify_fetch_admission,
    },
    protocol::fetch::ForgottenFetchPartition,
};

use super::{
    broker_execution::{ActiveBrokerSession, RoutedBrokerFetch},
    broker_session::BrokerSessionPlan,
    executor::{ActiveFetchReservation, DirectFetchExecutor},
    fault::{FetchExecutionError, RetainedFetchFault},
    prepared::PreparedFetchExecution,
};

impl DirectFetchExecutor {
    pub(super) fn submit_broker_plan(
        &mut self,
        driver: &DriverOwner,
        machine: &mut AssignedConsumerMachine,
        broker_id: BrokerId,
        mut prepared: Vec<PreparedFetchExecution>,
        plan: BrokerSessionPlan,
        now: Moment,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let forgotten = match build_forgotten(&plan) {
            Ok(forgotten) => forgotten,
            Err(failure) => {
                self.abort_unsent_plan(plan)?;
                return self.settle_one_and_restore(machine, broker_id, prepared, failure);
            }
        };
        if self.active.try_reserve(prepared.len()).is_err() {
            self.abort_unsent_plan(plan)?;
            self.restore_routed_batch(broker_id, prepared);
            return Ok(None);
        }
        let mut specs = Vec::new();
        let mut requests = Vec::new();
        let mut bytes = Vec::new();
        let mut fences = Vec::new();
        if specs.try_reserve_exact(prepared.len()).is_err()
            || requests.try_reserve_exact(prepared.len()).is_err()
            || bytes.try_reserve_exact(prepared.len()).is_err()
            || fences.try_reserve_exact(prepared.len()).is_err()
        {
            self.abort_unsent_plan(plan)?;
            self.restore_routed_batch(broker_id, prepared);
            return Ok(None);
        }
        specs.extend(
            prepared
                .iter()
                .map(|prepared| (prepared.fence(), prepared.hard_output_bytes)),
        );
        let reservations = match self.store.try_reserve_batch(&specs) {
            Ok(reservations) => reservations,
            Err(
                super::super::fetch_store::FetchStoreFailure::CountCapacity
                | super::super::fetch_store::FetchStoreFailure::ByteCapacity
                | super::super::fetch_store::FetchStoreFailure::AccountingOverflow,
            ) => {
                self.abort_unsent_plan(plan)?;
                self.restore_routed_batch(broker_id, prepared);
                return Ok(None);
            }
            Err(error) => {
                self.abort_unsent_plan(plan)?;
                let retained = prepared
                    .pop()
                    .unwrap_or_else(|| unreachable!("nonempty broker Fetch batch"));
                self.restore_routed_batch(broker_id, prepared);
                self.fault = Some(RetainedFetchFault::Prepared {
                    _prepared: retained,
                });
                return Err(FetchExecutionError::Store(error));
            }
        };
        for (prepared, reservation) in prepared.into_iter().zip(reservations) {
            let fence = prepared.fence();
            fences.push(fence);
            bytes.push(prepared.hard_output_bytes);
            requests.push(prepared.request);
            self.active
                .push(ActiveFetchReservation { fence, reservation });
        }
        match self
            .broker_calls
            .try_submit(driver, broker_id, requests, &forgotten, now)
        {
            BrokerFetchCallAdmission::Accepted => {
                self.active_broker_sessions.push(ActiveBrokerSession {
                    fences,
                    plan,
                    update: None,
                    reset: false,
                });
                Ok(None)
            }
            BrokerFetchCallAdmission::Backpressured(requests) => {
                self.abort_unsent_plan(plan)?;
                let prepared = self.rollback_broker_admission(requests, bytes)?;
                self.restore_routed_batch(broker_id, prepared);
                Ok(None)
            }
            BrokerFetchCallAdmission::Rejected(failure) => {
                self.abort_unsent_plan(plan)?;
                let (requests, source) = failure.into_parts();
                let prepared = self.rollback_broker_admission(requests, bytes)?;
                let failure = classify_fetch_admission(&source);
                self.settle_one_and_restore(machine, broker_id, prepared, failure)
            }
        }
    }

    fn rollback_broker_admission(
        &mut self,
        requests: Vec<PartitionFetchRequest>,
        bytes: Vec<usize>,
    ) -> Result<Vec<PreparedFetchExecution>, FetchExecutionError> {
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(requests.len())
            .map_err(|_| FetchExecutionError::BrokerSession)?;
        for (request, hard_output_bytes) in requests.into_iter().zip(bytes) {
            let request = PreparedFetchExecution::from_parts(request, hard_output_bytes);
            let (request, reservation) = self.take_active_for_admission(request)?;
            prepared.push(self.rollback_or_fault(request, reservation)?);
        }
        Ok(prepared)
    }

    fn settle_one_and_restore(
        &mut self,
        machine: &mut AssignedConsumerMachine,
        broker_id: BrokerId,
        mut prepared: Vec<PreparedFetchExecution>,
        failure: FetchFailure,
    ) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
        let selected = prepared
            .pop()
            .unwrap_or_else(|| unreachable!("nonempty broker Fetch batch"));
        self.restore_routed_batch(broker_id, prepared);
        settle(self, machine, selected, failure)
    }

    fn abort_unsent_plan(&mut self, plan: BrokerSessionPlan) -> Result<(), FetchExecutionError> {
        self.broker_sessions
            .as_mut()
            .unwrap_or_else(|| unreachable!("broker plan requires session owner"))
            .abort(plan, false)
            .map_err(|_error| FetchExecutionError::BrokerRouteCompletion)
    }

    pub(super) fn restore_routed(&mut self, broker_id: BrokerId, prepared: PreparedFetchExecution) {
        let (request, hard_output_bytes) = prepared.into_parts();
        self.routed.push(RoutedBrokerFetch {
            broker_id,
            request,
            hard_output_bytes,
        });
    }

    fn restore_routed_batch(&mut self, broker_id: BrokerId, prepared: Vec<PreparedFetchExecution>) {
        for prepared in prepared {
            self.restore_routed(broker_id, prepared);
        }
    }
}

fn build_forgotten(
    plan: &BrokerSessionPlan,
) -> Result<Vec<ForgottenFetchPartition<'_>>, FetchFailure> {
    let mut forgotten = Vec::new();
    forgotten
        .try_reserve_exact(plan.forgotten().len())
        .map_err(|_error| FetchFailure::DriverRejected)?;
    forgotten.extend(plan.forgotten().iter().map(|member| {
        ForgottenFetchPartition::new(
            member.topic(),
            member.topic_id(),
            member.position().partition().partition().get(),
        )
    }));
    Ok(forgotten)
}

fn settle(
    executor: &mut DirectFetchExecutor,
    machine: &mut AssignedConsumerMachine,
    prepared: PreparedFetchExecution,
    failure: FetchFailure,
) -> Result<Option<AssignedConsumerTransition>, FetchExecutionError> {
    match executor.settle_unadmitted(machine, prepared, failure)? {
        super::FetchSubmission::Settled(transition) => Ok(transition),
        _ => unreachable!("unadmitted broker Fetch settles immediately"),
    }
}
