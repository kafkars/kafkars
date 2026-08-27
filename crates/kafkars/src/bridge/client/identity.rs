//! Broker-issued cluster identity preflight and readiness observation.

use std::time::Instant;

use super::ClientEngine;
use crate::error::Error as KafkaError;

pub(super) fn verify_startup(
    client: ClientEngine,
    identity_deadline: Option<Instant>,
) -> Result<ClientEngine, KafkaError> {
    if let Some(deadline) = identity_deadline {
        if let Err(error) = client.identity_probe(deadline).wait() {
            let _shutdown_result = client.shutdown.begin().wait();
            return Err(error);
        }
    }
    Ok(client)
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
