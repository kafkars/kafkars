//! Linear join between one waiting producer record and one driver topic view.

use crate::{
    driver::{
        DriverOwner, ProducerTopicView, ProducerTopicViewCall,
        TopicPartitionCountAdmissionFailureKind, TopicPartitionCountFailure,
    },
    producer::{
        ProducerPartitionSource, ProducerPartitioningFailure, ProducerPartitioningRequest,
        ingress::ProducerShardData,
    },
};

use super::super::EngineHostError;

impl ProducerPartitionSource for ProducerTopicView {
    fn leader_broker_id(&self, partition: kafka_client_core::PartitionIndex) -> Option<i32> {
        self.leader_broker_id(partition)
    }
}

/// Exact waiting and driver ownership retained until metadata settles.
pub(in crate::engine_host) struct ProducerPartitioningCall {
    request: ProducerPartitioningRequest,
    call: ProducerTopicViewCall,
}

impl ProducerPartitioningCall {
    pub(in crate::engine_host) const fn deadline(&self) -> crate::clock::OperationDeadline {
        self.request.deadline()
    }
}

pub(in crate::engine_host) fn admit(
    driver: &DriverOwner,
    retained: &mut Option<ProducerPartitioningCall>,
    data: &mut ProducerShardData,
) -> Result<bool, EngineHostError> {
    if retained.is_some() {
        return Ok(false);
    }
    let Some(request) = data
        .take_partitioning_request()
        .map_err(EngineHostError::Producer)?
    else {
        return Ok(false);
    };
    match ProducerTopicViewCall::submit(driver, request.topic(), request.deadline().transport()) {
        Ok(call) => {
            *retained = Some(ProducerPartitioningCall { request, call });
            Ok(true)
        }
        Err(error) if error.kind() == TopicPartitionCountAdmissionFailureKind::Full => {
            data.restore_partitioning_request(request)
                .map_err(EngineHostError::Producer)?;
            Ok(false)
        }
        Err(_error) => {
            data.apply_partitioning_failure(
                request,
                ProducerPartitioningFailure::MetadataUnavailable { broker_code: None },
            )
            .map_err(EngineHostError::Producer)?;
            Ok(true)
        }
    }
}

pub(in crate::engine_host) fn apply_ready(
    retained: &mut Option<ProducerPartitioningCall>,
    data: &mut ProducerShardData,
) -> Result<bool, EngineHostError> {
    let Some(result) = retained
        .as_mut()
        .and_then(|retained| retained.call.try_terminal())
    else {
        return Ok(false);
    };
    let Some(ProducerPartitioningCall { request, call: _ }) = retained.take() else {
        unreachable!("terminal producer topic view retains its request")
    };
    match result {
        Ok(view) => {
            data.apply_partitioning_view(request, &view)
                .map_err(EngineHostError::Producer)?;
        }
        Err(error) => {
            data.apply_partitioning_failure(request, normalize_failure(error))
                .map_err(EngineHostError::Producer)?;
        }
    }
    Ok(true)
}

pub(in crate::engine_host) fn discard_after_driver_shutdown(
    retained: &mut Option<ProducerPartitioningCall>,
) {
    if let Some(retained) = retained.take() {
        retained.call.discard_after_driver_shutdown();
    }
}

fn normalize_failure(failure: TopicPartitionCountFailure) -> ProducerPartitioningFailure {
    match failure {
        TopicPartitionCountFailure::Deadline => ProducerPartitioningFailure::DeadlineElapsed,
        TopicPartitionCountFailure::Broker(error_code) => {
            ProducerPartitioningFailure::MetadataUnavailable {
                broker_code: Some(error_code),
            }
        }
        TopicPartitionCountFailure::Unavailable
        | TopicPartitionCountFailure::Refresh
        | TopicPartitionCountFailure::Malformed
        | TopicPartitionCountFailure::Allocation
        | TopicPartitionCountFailure::QueryCapacity(_)
        | TopicPartitionCountFailure::Capacity { .. }
        | TopicPartitionCountFailure::Draining
        | TopicPartitionCountFailure::TopicMismatch
        | TopicPartitionCountFailure::Completion => {
            ProducerPartitioningFailure::MetadataUnavailable { broker_code: None }
        }
    }
}
