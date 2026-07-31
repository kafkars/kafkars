//! Exact prepared ownership and local admission for one partition Fetch.

use kafka_client_core::{AssignedConsumerEffect, FetchFence, Moment, NextFetchOffset};
use kafka_driver::RoutedCall;
use kafka_wire::{FetchRequest, FetchResponse as WireFetchResponse};

use crate::{
    clock::OperationDeadline,
    driver::DriverOwner,
    protocol::{
        consumer::remaining_timeout_ms,
        fetch::{
            FetchDecodeLimits, FetchIsolation, FetchRequestFailure, FetchRequestSettings,
            FetchSessionRequest, fetch_request_with_session,
        },
    },
};

use super::submission::FetchSubmitError;

/// One core-selected Fetch paired with engine catalog, limits, and deadline facts.
#[must_use = "a prepared partition Fetch must be submitted or terminally settled"]
pub(crate) struct PartitionFetchRequest {
    fence: FetchFence,
    next_offset: NextFetchOffset,
    topic: String,
    settings: FetchRequestSettings,
    session: FetchSessionRequest,
    decode_limits: FetchDecodeLimits,
    operation_deadline: OperationDeadline,
}

impl PartitionFetchRequest {
    #[allow(
        clippy::too_many_arguments,
        reason = "the prepared Fetch owner carries every exact execution fact"
    )]
    pub(crate) fn from_fetch_ready_parts(
        fence: FetchFence,
        next_offset: NextFetchOffset,
        topic: String,
        settings: FetchRequestSettings,
        decode_limits: FetchDecodeLimits,
        operation_deadline: OperationDeadline,
    ) -> Self {
        Self {
            fence,
            next_offset,
            topic,
            settings,
            session: FetchSessionRequest::LEGACY,
            decode_limits,
            operation_deadline,
        }
    }

    pub(crate) fn from_effect(
        effect: AssignedConsumerEffect,
        topic: String,
        settings: FetchRequestSettings,
        decode_limits: FetchDecodeLimits,
        operation_deadline: OperationDeadline,
    ) -> Result<Self, FetchRequestPreparationError> {
        let AssignedConsumerEffect::FetchReady { fence, next_offset } = effect else {
            return Err(FetchRequestPreparationError::UnexpectedEffect);
        };
        Ok(Self {
            fence,
            next_offset,
            topic,
            settings,
            session: FetchSessionRequest::LEGACY,
            decode_limits,
            operation_deadline,
        })
    }

    pub(crate) const fn fence(&self) -> FetchFence {
        self.fence
    }

    pub(crate) const fn next_offset(&self) -> NextFetchOffset {
        self.next_offset
    }

    pub(crate) fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) const fn operation_deadline(&self) -> OperationDeadline {
        self.operation_deadline
    }

    pub(crate) const fn decode_limits(&self) -> FetchDecodeLimits {
        self.decode_limits
    }

    pub(crate) const fn isolation(&self) -> Option<FetchIsolation> {
        self.settings.isolation()
    }

    pub(super) const fn settings(&self) -> FetchRequestSettings {
        self.settings
    }

    pub(crate) const fn session(&self) -> FetchSessionRequest {
        self.session
    }

    pub(crate) fn bind_session(&mut self, session: FetchSessionRequest) {
        self.session = session;
    }
}

/// Preparation rejected a non-Fetch effect without consuming it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchRequestPreparationError {
    UnexpectedEffect,
}

pub(super) struct AcceptedFetchCall {
    pub(super) request: PartitionFetchRequest,
    pub(super) call: RoutedCall<WireFetchResponse>,
}

/// Definitely-unsent request construction or driver admission failure.
#[must_use = "the exact rejected Fetch request remains owned"]
pub(crate) struct FetchAdmissionFailure {
    request: PartitionFetchRequest,
    source: FetchAdmissionFailureSource,
}

impl FetchAdmissionFailure {
    pub(super) fn deadline_elapsed(request: PartitionFetchRequest) -> Self {
        Self {
            request,
            source: FetchAdmissionFailureSource::DeadlineElapsed,
        }
    }

    pub(crate) fn into_parts(self) -> (PartitionFetchRequest, FetchAdmissionFailureSource) {
        (self.request, self.source)
    }
}

/// Exact local boundary that rejected the prepared Fetch.
#[derive(Debug)]
pub(crate) enum FetchAdmissionFailureSource {
    DeadlineElapsed,
    EmptyBrokerBatch,
    InconsistentBrokerBatch,
    Request(FetchRequestFailure),
    Driver(FetchSubmitError),
}

/// Result of a capacity-preflighted attempt to submit one exact Fetch.
#[must_use = "backpressured or rejected Fetch ownership must be handled"]
pub(crate) enum FetchCallAdmission {
    Accepted,
    Backpressured(PartitionFetchRequest),
    Rejected(FetchAdmissionFailure),
}

#[allow(
    clippy::result_large_err,
    reason = "local rejection must return the exact linear prepared Fetch without allocation"
)]
pub(super) fn submit_partition_fetch(
    driver: &DriverOwner,
    request: PartitionFetchRequest,
    now: Moment,
) -> Result<AcceptedFetchCall, FetchAdmissionFailure> {
    let (generated, partition) = match generated_fetch_request(&request, now) {
        Ok(generated) => generated,
        Err(source) => {
            return Err(FetchAdmissionFailure { request, source });
        }
    };
    let call = match driver.submit_tracked_fetch(
        &request.topic,
        partition,
        generated,
        request.operation_deadline.transport(),
    ) {
        Ok(call) => call,
        Err(source) => {
            return Err(FetchAdmissionFailure {
                request,
                source: FetchAdmissionFailureSource::Driver(source),
            });
        }
    };
    Ok(AcceptedFetchCall { request, call })
}

pub(super) fn generated_fetch_request(
    request: &PartitionFetchRequest,
    now: Moment,
) -> Result<(FetchRequest, i32), FetchAdmissionFailureSource> {
    let remaining = remaining_timeout_ms(now, request.operation_deadline.core())
        .map_err(|_error| FetchAdmissionFailureSource::DeadlineElapsed)?;
    let remaining =
        u32::try_from(remaining).map_err(|_error| FetchAdmissionFailureSource::DeadlineElapsed)?;
    let partition = request.fence.position().partition().partition().get();
    let generated = fetch_request_with_session(
        &request.topic,
        partition,
        request.next_offset.get(),
        request.settings.cap_max_wait_ms(remaining),
        request.session,
    )
    .map_err(FetchAdmissionFailureSource::Request)?;
    let partition = i32::try_from(partition).map_err(|_error| {
        FetchAdmissionFailureSource::Request(FetchRequestFailure::PartitionOutOfRange {
            actual: partition,
        })
    })?;
    Ok((generated, partition))
}
