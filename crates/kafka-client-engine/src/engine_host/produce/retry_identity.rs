//! One replacement Produce retained until newer metadata revalidates its topic UUID.

use kafka_client_core::{Moment, ProducerAttemptFailureKind};

use crate::{
    driver::{DriverOwner, TopicRouteViewCall, TrackedProduceCalls},
    producer::{execution::PreparedProduceSubmission, ingress::ProducerShardData},
};

use super::{EngineHostError, reject_execution};

/// One replacement execution retained until newer metadata revalidates its topic UUID.
pub(in crate::engine_host) struct ProducerRetryIdentityCall {
    submission: PreparedProduceSubmission,
    expected_topic_uuid: [u8; 16],
    call: Option<TopicRouteViewCall>,
    validated: bool,
}

impl ProducerRetryIdentityCall {
    #[expect(
        clippy::result_large_err,
        reason = "rejection returns the exact caller-owned prepared Produce submission"
    )]
    pub(in crate::engine_host) fn submit(
        driver: &DriverOwner,
        submission: PreparedProduceSubmission,
    ) -> Result<Self, PreparedProduceSubmission> {
        let Some((expected_topic_uuid, observed_generation)) = submission.retry_topic_identity()
        else {
            return Err(submission);
        };
        let call = match TopicRouteViewCall::submit_newer_than(
            driver,
            submission.topic(),
            observed_generation,
            submission.deadline().transport(),
        ) {
            Ok(call) => call,
            Err(_error) => return Err(submission),
        };
        Ok(Self {
            submission,
            expected_topic_uuid,
            call: Some(call),
            validated: false,
        })
    }

    pub(in crate::engine_host) const fn deadline(&self) -> crate::clock::OperationDeadline {
        self.submission.deadline()
    }

    fn poll(&mut self) -> Option<Result<(), ProducerAttemptFailureKind>> {
        if self.validated {
            return Some(Ok(()));
        }
        let result = self.call.as_mut()?.try_terminal()?;
        self.call = None;
        match result {
            Ok(view)
                if view.kafka_topic_id() == Some(self.expected_topic_uuid)
                    && self.submission.record_retry_topic_identity(
                        self.expected_topic_uuid,
                        view.metadata_generation(),
                    ) =>
            {
                self.validated = true;
                Some(Ok(()))
            }
            Ok(view) if view.kafka_topic_id() != Some(self.expected_topic_uuid) => {
                Some(Err(ProducerAttemptFailureKind::Identity))
            }
            Ok(_) | Err(_) => Some(Err(ProducerAttemptFailureKind::RouteUnavailable)),
        }
    }

    const fn is_validated(&self) -> bool {
        self.validated
    }

    fn into_submission(self) -> PreparedProduceSubmission {
        self.submission
    }

    fn discard(mut self) {
        if let Some(call) = self.call.take() {
            call.discard_after_driver_shutdown();
        }
    }
}

pub(super) fn admit(
    driver: &DriverOwner,
    calls: &mut TrackedProduceCalls,
    retained: &mut Option<ProducerRetryIdentityCall>,
    data: &mut ProducerShardData,
    now: Moment,
) -> Result<Option<bool>, EngineHostError> {
    if !retained
        .as_ref()
        .is_some_and(ProducerRetryIdentityCall::is_validated)
    {
        return Ok(retained.as_ref().map(|_| false));
    }
    if !calls.broker_admission_available(None) {
        return Ok(Some(false));
    }
    let Some(permit) = calls.try_reserve() else {
        return Ok(Some(false));
    };
    let submission = retained
        .take()
        .unwrap_or_else(|| unreachable!("validated retry identity retains its submission"))
        .into_submission();
    let request_records = u64::from(submission.record_count());
    let request_bytes = submission.encoded_record_bytes();
    let (execution, deadline, materialized) = submission.into_parts();
    match permit.submit(driver, execution, deadline, materialized, now) {
        Ok(accepted) => {
            data.record_produce_request(
                1,
                request_records,
                request_bytes,
                calls.in_flight_request_count(),
                calls.max_broker_in_flight_request_count(),
            );
            data.apply_produce_driver_input(now, accepted.driver_accepted())
                .map_err(EngineHostError::Producer)?;
            accepted.confirm_receipt();
        }
        Err(rejection) => {
            let failure = rejection.failure_kind();
            drop(rejection);
            reject_execution(data, execution, now, failure)?;
        }
    }
    Ok(Some(true))
}

pub(in crate::engine_host) fn apply_ready(
    retained: &mut Option<ProducerRetryIdentityCall>,
    data: &mut ProducerShardData,
    now: Moment,
) -> Result<bool, EngineHostError> {
    let Some(result) = retained.as_mut().and_then(ProducerRetryIdentityCall::poll) else {
        return Ok(false);
    };
    match result {
        Ok(()) => Ok(true),
        Err(failure) => {
            let submission = retained
                .take()
                .unwrap_or_else(|| unreachable!("terminal identity lookup retains submission"))
                .into_submission();
            reject_execution(data, submission.execution(), now, failure)?;
            Ok(true)
        }
    }
}

pub(in crate::engine_host) fn discard_after_driver_shutdown(
    retained: &mut Option<ProducerRetryIdentityCall>,
) {
    if let Some(retained) = retained.take() {
        retained.discard();
    }
}
