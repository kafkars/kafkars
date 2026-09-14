//! Broker-issued cluster identity preflight and readiness observation.

use std::{
    thread,
    time::{Duration, Instant},
};

use super::ClientEngine;
use crate::error::{DeliveryStatus, Error as KafkaError, ErrorKind, RetryAdvice};

const IDENTITY_ADMISSION_RETRY_BACKOFF: Duration = Duration::from_millis(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::bridge) enum IdentityProbeFailureDecision {
    Retry(Duration),
    DeadlineElapsed,
    Return,
}

pub(super) fn verify_startup(
    client: ClientEngine,
    identity_deadline: Option<Instant>,
) -> Result<ClientEngine, KafkaError> {
    if let Some(deadline) = identity_deadline {
        if let Err(error) = verify_identity_until(&client, deadline) {
            let _shutdown_result = client.shutdown.begin().wait();
            return Err(error);
        }
    }
    Ok(client)
}

fn verify_identity_until(client: &ClientEngine, deadline: Instant) -> Result<(), KafkaError> {
    loop {
        if Instant::now() >= deadline {
            return Err(identity_deadline_elapsed());
        }
        match client.identity_probe(deadline).wait() {
            Ok(_description) => return Ok(()),
            Err(error) => match identity_probe_failure_decision(&error, Instant::now(), deadline) {
                IdentityProbeFailureDecision::Retry(backoff) => thread::sleep(backoff),
                IdentityProbeFailureDecision::DeadlineElapsed => {
                    return Err(identity_deadline_elapsed());
                }
                IdentityProbeFailureDecision::Return => return Err(error),
            },
        }
    }
}

pub(in crate::bridge) fn identity_probe_failure_decision(
    error: &KafkaError,
    now: Instant,
    deadline: Instant,
) -> IdentityProbeFailureDecision {
    if error.retry_advice() != RetryAdvice::RetrySafe
        || error.delivery_status() != Some(DeliveryStatus::NotSent)
    {
        return IdentityProbeFailureDecision::Return;
    }
    let remaining = deadline.saturating_duration_since(now);
    if remaining.is_zero() {
        IdentityProbeFailureDecision::DeadlineElapsed
    } else {
        IdentityProbeFailureDecision::Retry(IDENTITY_ADMISSION_RETRY_BACKOFF.min(remaining))
    }
}

fn identity_deadline_elapsed() -> KafkaError {
    KafkaError::new(
        ErrorKind::Timeout,
        "cluster identity preflight deadline elapsed",
    )
    .with_delivery_status(DeliveryStatus::NotSent)
    .with_safe_retry()
}

impl ClientEngine {
    /// Returns the retained broker-issued cluster identity requirement.
    pub(crate) fn expected_cluster_id(&self) -> Option<&str> {
        self.expected_cluster_id.as_deref()
    }

    /// Immediately admits one bounded point-in-time readiness probe.
    pub(crate) fn ready(
        &self,
        deadline: Instant,
    ) -> super::super::admin_describe_operation::AdminDescribeCluster {
        self.identity_probe(deadline)
    }

    fn identity_probe(
        &self,
        deadline: Instant,
    ) -> super::super::admin_describe_operation::AdminDescribeCluster {
        self.admin()
            .submit_describe_cluster_until(deadline)
            .with_expected_cluster_id(self.expected_cluster_id.clone())
    }
}
