//! Client-owned construction seam for one bounded share membership.

use std::time::Duration;

use kafka_client_engine::share::ShareConsumerStartCapture;

use crate::{KafkaError, ShareConsumerFetchConfig, bridge::share_consumer::ShareConsumerEngine};

use super::ClientEngine;

impl ClientEngine {
    pub(crate) fn capture_share_consumer_start(
        &self,
        timeout: Duration,
    ) -> Result<ShareConsumerStartCapture, KafkaError> {
        self.inner
            .capture_share_consumer_start(timeout)
            .map_err(crate::bridge::share_consumer::translate_registration_kind)
    }

    pub(crate) fn register_share_consumer(
        &self,
        capture: ShareConsumerStartCapture,
        group: &str,
        rack: Option<&str>,
        topics: &[String],
        fetch: ShareConsumerFetchConfig,
        close_timeout: Duration,
    ) -> Result<ShareConsumerEngine, KafkaError> {
        ShareConsumerEngine::register(
            &self.inner,
            capture,
            group,
            rack,
            topics,
            fetch,
            close_timeout,
        )
    }
}
