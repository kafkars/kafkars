//! Exact-broker Fetch materialization and tracked-call admission.

use std::time::Instant;

use kafka_client_core::Moment;
use kafka_wire::FetchRequest;

use crate::{
    driver::DriverOwner,
    protocol::{
        consumer::remaining_timeout_ms,
        fetch::{BrokerFetchPartition, ForgottenFetchPartition, broker_fetch_request},
    },
};

use super::{
    admission::{FetchAdmissionFailureSource, PartitionFetchRequest},
    route::BrokerId,
};

pub(super) struct AcceptedBrokerFetchCall {
    pub(super) requests: Vec<PartitionFetchRequest>,
    pub(super) call: kafka_driver::RoutedCall<kafka_wire::FetchResponse>,
}

#[must_use = "the exact rejected broker Fetch batch remains owned"]
pub(crate) struct BrokerFetchAdmissionFailure {
    requests: Vec<PartitionFetchRequest>,
    source: FetchAdmissionFailureSource,
}

impl BrokerFetchAdmissionFailure {
    pub(super) const fn new(
        requests: Vec<PartitionFetchRequest>,
        source: FetchAdmissionFailureSource,
    ) -> Self {
        Self { requests, source }
    }

    pub(crate) fn into_parts(self) -> (Vec<PartitionFetchRequest>, FetchAdmissionFailureSource) {
        (self.requests, self.source)
    }
}

#[allow(
    clippy::result_large_err,
    reason = "local rejection must return the exact prepared Fetch batch without allocation"
)]
pub(super) fn submit_broker_fetch_batch(
    driver: &DriverOwner,
    broker_id: BrokerId,
    requests: Vec<PartitionFetchRequest>,
    forgotten: &[ForgottenFetchPartition<'_>],
    now: Moment,
) -> Result<AcceptedBrokerFetchCall, BrokerFetchAdmissionFailure> {
    let (generated, deadline) = match generated_broker_fetch_request(&requests, forgotten, now) {
        Ok(generated) => generated,
        Err(source) => return Err(BrokerFetchAdmissionFailure { requests, source }),
    };
    let call = match driver.submit_tracked_broker_fetch(broker_id, generated, deadline) {
        Ok(call) => call,
        Err(source) => {
            return Err(BrokerFetchAdmissionFailure {
                requests,
                source: FetchAdmissionFailureSource::Driver(source),
            });
        }
    };
    Ok(AcceptedBrokerFetchCall { requests, call })
}

pub(super) fn generated_broker_fetch_request(
    requests: &[PartitionFetchRequest],
    forgotten: &[ForgottenFetchPartition<'_>],
    now: Moment,
) -> Result<(FetchRequest, Instant), FetchAdmissionFailureSource> {
    let Some(first) = requests.first() else {
        return Err(FetchAdmissionFailureSource::EmptyBrokerBatch);
    };
    if requests.iter().any(|request| {
        request.settings() != first.settings() || request.session() != first.session()
    }) {
        return Err(FetchAdmissionFailureSource::InconsistentBrokerBatch);
    }
    let core_deadline = requests
        .iter()
        .map(|request| request.operation_deadline().core())
        .min()
        .unwrap_or_else(|| unreachable!("nonempty broker Fetch batch"));
    let transport_deadline = requests
        .iter()
        .map(|request| request.operation_deadline().transport())
        .min()
        .unwrap_or_else(|| unreachable!("nonempty broker Fetch batch"));
    let remaining = remaining_timeout_ms(now, core_deadline)
        .map_err(|_error| FetchAdmissionFailureSource::DeadlineElapsed)?;
    let remaining =
        u32::try_from(remaining).map_err(|_error| FetchAdmissionFailureSource::DeadlineElapsed)?;
    let active = requests
        .iter()
        .map(|request| {
            let route = request
                .topic_route()
                .ok_or(FetchAdmissionFailureSource::InconsistentBrokerBatch)?;
            Ok(BrokerFetchPartition::new(
                request.topic(),
                route.topic_id(),
                route.leader_epoch(),
                request.fence().position().partition().partition().get(),
                request.next_offset().get(),
            ))
        })
        .collect::<Result<Vec<_>, FetchAdmissionFailureSource>>()?;
    let generated = broker_fetch_request(
        &active,
        forgotten,
        first.settings().cap_max_wait_ms(remaining),
        first.session(),
    )
    .map_err(FetchAdmissionFailureSource::Request)?;
    Ok((generated, transport_deadline))
}
