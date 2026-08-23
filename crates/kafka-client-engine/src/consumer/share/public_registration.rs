//! Public linear registration ownership for one hosted share member.

use std::{cell::Cell, marker::PhantomData, sync::Arc, time::Duration};

use kafka_client_core::GroupId;

use super::{
    port::ShareConsumerPort,
    public_registration_error::{
        ShareConsumerRegistrationError, ShareConsumerRegistrationErrorKind, registration_error,
        registration_error_kind,
    },
};
use crate::{EngineShareConsumerFetchConfig, clock::DeadlineCapture};

/// Inert names and close policy for one share-member registration.
#[derive(Debug)]
pub struct ShareConsumerRegistration {
    pub(super) group: Arc<str>,
    pub(super) rack: Option<Arc<str>>,
    pub(super) topics: Vec<Arc<str>>,
    pub(super) fetch: EngineShareConsumerFetchConfig,
    pub(super) close_timeout: Duration,
}

impl ShareConsumerRegistration {
    /// Creates one registration with a caller-ordered topic subscription.
    pub fn new(group: Arc<str>, topics: Vec<Arc<str>>) -> Self {
        Self {
            group,
            rack: None,
            topics,
            fetch: EngineShareConsumerFetchConfig::default(),
            close_timeout: Duration::from_secs(30),
        }
    }

    /// Sets the optional rack spelling sent by `ShareGroupHeartbeat`.
    pub fn with_rack(mut self, rack: Arc<str>) -> Self {
        self.rack = Some(rack);
        self
    }

    /// Sets immutable `ShareFetch` request and attempt policy.
    pub const fn with_fetch_config(mut self, fetch: EngineShareConsumerFetchConfig) -> Self {
        self.fetch = fetch;
        self
    }

    /// Sets the end-to-end explicit close duration.
    pub const fn with_close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }

    /// Returns the requested group spelling.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Returns the requested rack spelling.
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Returns the caller-ordered topic subscription.
    pub fn topics(&self) -> &[Arc<str>] {
        &self.topics
    }

    /// Returns immutable `ShareFetch` request and attempt policy.
    pub const fn fetch_config(&self) -> EngineShareConsumerFetchConfig {
        self.fetch
    }

    /// Returns the explicit close duration.
    pub const fn close_timeout(&self) -> Duration {
        self.close_timeout
    }
}

/// Capture-first membership deadline bound to one engine registry.
#[must_use = "a captured share start should be admitted or discarded"]
pub struct ShareConsumerStartCapture {
    port: ShareConsumerPort,
    capture: DeadlineCapture,
}

impl core::fmt::Debug for ShareConsumerStartCapture {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ShareConsumerStartCapture")
            .finish_non_exhaustive()
    }
}

impl ShareConsumerStartCapture {
    pub(in crate::consumer) fn capture(
        port: ShareConsumerPort,
        timeout: Duration,
    ) -> Result<Self, ShareConsumerRegistrationErrorKind> {
        let capture = port
            .capture_deadline_after(timeout)
            .map_err(|_error| ShareConsumerRegistrationErrorKind::InvalidInput)?;
        if timeout.is_zero() {
            return Err(ShareConsumerRegistrationErrorKind::InvalidInput);
        }
        Ok(Self { port, capture })
    }
}

/// Unique ownership of one hosted share-member registration.
#[must_use = "the share member remains retained by its engine host"]
pub struct ShareConsumerHandle {
    pub(in crate::consumer) group_id: GroupId,
    pub(in crate::consumer) port: ShareConsumerPort,
    pub(super) lifetime: Arc<dyn Send + Sync>,
    pub(super) close_timeout: Duration,
    pub(super) startup_wake_failed: bool,
    pub(super) _not_sync: PhantomData<Cell<()>>,
}

impl ShareConsumerHandle {
    pub(in crate::consumer) fn try_register_started(
        port: ShareConsumerPort,
        lifetime: Arc<dyn Send + Sync>,
        capture: ShareConsumerStartCapture,
        registration: ShareConsumerRegistration,
    ) -> Result<Self, ShareConsumerRegistrationError> {
        let ShareConsumerRegistration {
            group,
            rack,
            topics,
            fetch,
            close_timeout,
        } = registration;
        let ShareConsumerStartCapture {
            port: capture_port,
            capture,
        } = capture;
        if !capture_port.shares_registry_with(&port) {
            return Err(registration_error(
                ShareConsumerRegistrationErrorKind::Internal,
                group,
                rack,
                topics,
                fetch,
                close_timeout,
            ));
        }
        let accepted = port
            .try_register_started(group, rack, topics, fetch, capture)
            .map_err(|failure| {
                registration_error(
                    registration_error_kind(failure.source),
                    failure.group,
                    failure.rack,
                    failure.topics,
                    *failure.fetch,
                    close_timeout,
                )
            })?;
        Ok(Self {
            group_id: accepted.group_id(),
            port,
            lifetime,
            close_timeout,
            startup_wake_failed: accepted.wake_failed(),
            _not_sync: PhantomData,
        })
    }

    /// Reports advisory wake degradation after accepted membership ownership.
    pub const fn startup_wake_failed(&self) -> bool {
        self.startup_wake_failed
    }
}

impl core::fmt::Debug for ShareConsumerHandle {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ShareConsumerHandle")
            .field("group_id", &self.group_id)
            .finish_non_exhaustive()
    }
}
