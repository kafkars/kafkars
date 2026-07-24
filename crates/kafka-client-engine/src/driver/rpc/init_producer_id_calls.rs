//! One-slot tracked ownership and semantic normalization of producer identity calls.

use std::{error::Error, fmt};

use kafka_client_core::{Moment, ProducerIdentityGeneration, ProducerInput};
use kafka_driver::{CallFailure, CompletionError, RequestError, RoutedCall, SubmitError};
use kafka_wire::InitProducerIdResponse;

use crate::{
    clock::OperationDeadline,
    protocol::init_producer_id::{
        InitProducerIdResponseFailure, nontransactional_init_producer_id_request,
        normalize_init_producer_id_response,
    },
};

use super::super::DriverOwner;

struct ProducerIdentityCallEntry {
    generation: ProducerIdentityGeneration,
    call: RoutedCall<InitProducerIdResponse>,
}

pub(crate) struct ProducerIdentityCallPermit<'a> {
    slot: &'a mut Option<ProducerIdentityCallEntry>,
}

impl ProducerIdentityCallPermit<'_> {
    pub(crate) fn submit(
        self,
        driver: &DriverOwner,
        generation: ProducerIdentityGeneration,
        deadline: OperationDeadline,
    ) -> Result<(), SubmitError> {
        let request = nontransactional_init_producer_id_request();
        let call = driver.submit_tracked_init_producer_id(request, deadline.transport())?;
        *self.slot = Some(ProducerIdentityCallEntry { generation, call });
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ProducerIdentityCompletionFailure {
    generation: ProducerIdentityGeneration,
    source: CompletionError,
}

impl fmt::Display for ProducerIdentityCompletionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "tracked producer identity generation {} failed: {}",
            self.generation.get(),
            self.source,
        )
    }
}

impl Error for ProducerIdentityCompletionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Default)]
pub(crate) struct TrackedProducerIdentityCalls {
    call: Option<ProducerIdentityCallEntry>,
}

impl TrackedProducerIdentityCalls {
    pub(crate) const fn new() -> Self {
        Self { call: None }
    }

    pub(crate) fn try_reserve(&mut self) -> Option<ProducerIdentityCallPermit<'_>> {
        self.call.is_none().then_some(ProducerIdentityCallPermit {
            slot: &mut self.call,
        })
    }

    pub(crate) const fn retained_count(&self) -> usize {
        if self.call.is_some() { 1 } else { 0 }
    }

    pub(crate) fn poll_ready(
        &mut self,
        now: Moment,
    ) -> Result<Option<ProducerInput>, ProducerIdentityCompletionFailure> {
        let Some(result) = self.call.as_ref().and_then(|call| call.call.try_result()) else {
            return Ok(None);
        };
        let Some(call) = self.call.take() else {
            return Ok(None);
        };
        let outcome = result.map_err(|source| ProducerIdentityCompletionFailure {
            generation: call.generation,
            source,
        })?;
        let (result, _selected_version, _route_token) = outcome.into_parts();
        Ok(Some(normalize_terminal(call.generation, now, result)))
    }

    pub(crate) fn discard_after_driver_shutdown(&mut self) {
        self.call = None;
    }
}

pub(super) fn normalize_terminal(
    generation: ProducerIdentityGeneration,
    now: Moment,
    result: Result<InitProducerIdResponse, RequestError>,
) -> ProducerInput {
    match result {
        Ok(response) => match normalize_init_producer_id_response(&response) {
            Ok(identity) => ProducerInput::ProducerIdentityAcquired {
                generation,
                producer_id: identity.producer_id(),
                producer_epoch: identity.producer_epoch(),
                now,
            },
            Err(InitProducerIdResponseFailure::Broker { code }) => {
                ProducerInput::ProducerIdentityFailed {
                    generation,
                    broker_code: Some(code),
                }
            }
            Err(
                InitProducerIdResponseFailure::InvalidProducerId { .. }
                | InitProducerIdResponseFailure::InvalidProducerEpoch { .. },
            ) => ProducerInput::ProducerIdentityFailed {
                generation,
                broker_code: None,
            },
        },
        Err(RequestError::Rejected {
            failure: CallFailure::DeadlineExceeded,
            ..
        }) => ProducerInput::ProducerIdentityDeadlineElapsed { generation, now },
        Err(_other) => ProducerInput::ProducerIdentityRequestFailed { generation, now },
    }
}
