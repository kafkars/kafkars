//! Public linear ownership of one bounded group-consumer registry registration.

use super::{
    GroupConsumerBatch, GroupConsumerPort, GroupConsumerPortRegistrationCategory,
    GroupConsumerStartAccepted, GroupConsumerStartCapture, GroupConsumerStartError,
    GroupConsumerTryTakeBatchError,
    group::{GroupConsumerCloseAuthority, GroupConsumerPortRegistrationAccepted},
    group_registration_request::GroupConsumerRegistration,
};
use kafka_client_core::{ClassicGroupTiming, ClassicHeartbeatPolicy, ClassicRejoinPolicy};
use std::{cell::Cell, marker::PhantomData, sync::Arc, time::Duration};
const DEFAULT_SESSION_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_REBALANCE_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_HEARTBEAT_INTERVAL_TICKS: u64 = 3_000_000_000;
const DEFAULT_HEARTBEAT_ATTEMPT_TIMEOUT_TICKS: u64 = 10_000_000_000;
const DEFAULT_REJOIN_BACKOFF_TICKS: u64 = 1_000_000_000;
const DEFAULT_REJOIN_ATTEMPT_TIMEOUT_TICKS: u64 = 30_000_000_000;
/// Stable reason bounded group registration did not transfer ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupConsumerRegistrationErrorKind {
    /// Engine or group admission has closed.
    Closed,
    /// Another owner temporarily holds the group registry.
    Contended,
    /// A bounded group count or retained-byte limit is full.
    Backpressure,
    /// Group or topic input is outside the supported bounded domain.
    InvalidInput,
    /// Internal ownership could not be acquired consistently.
    Internal,
}
/// Rejected registration retaining the exact request.
#[derive(Debug)]
#[must_use = "registration rejection retains the exact request"]
pub struct GroupConsumerRegistrationError {
    kind: GroupConsumerRegistrationErrorKind,
    request: GroupConsumerRegistration,
}
impl GroupConsumerRegistrationError {
    pub(crate) const fn new(
        kind: GroupConsumerRegistrationErrorKind,
        request: GroupConsumerRegistration,
    ) -> Self {
        Self { kind, request }
    }

    /// Returns the stable rejection category.
    pub const fn kind(&self) -> GroupConsumerRegistrationErrorKind {
        self.kind
    }

    /// Returns the exact rejected registration request.
    pub fn into_request(self) -> GroupConsumerRegistration {
        self.request
    }
}
impl core::fmt::Display for GroupConsumerRegistrationError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "group-consumer registration rejected: {:?}",
            self.kind
        )
    }
}
impl std::error::Error for GroupConsumerRegistrationError {}
/// Unique ownership of one registered group-consumer entry.
///
/// Registration reserves catalog and Fetch ownership; observation transfers only authorized delivery.
/// Drop invents neither `LeaveGroup` nor publication-fenced removal.
#[must_use = "the registered group remains retained by its engine host"]
pub struct GroupConsumerHandle {
    pub(super) group_id: kafka_client_core::GroupId,
    pub(super) port: GroupConsumerPort,
    pub(super) lifetime: Arc<dyn Send + Sync>,
    pub(super) close_authority: Arc<GroupConsumerCloseAuthority>,
    pub(super) _not_sync: PhantomData<Cell<()>>,
}
impl core::fmt::Debug for GroupConsumerHandle {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("GroupConsumerHandle")
            .field("group_id", &self.group_id)
            .finish_non_exhaustive()
    }
}
impl GroupConsumerHandle {
    #[cfg(test)]
    pub(crate) fn from_registered_for_test(
        port: GroupConsumerPort,
        lifetime: Arc<dyn Send + Sync>,
        group_id: kafka_client_core::GroupId,
    ) -> Self {
        Self {
            group_id,
            port,
            lifetime,
            close_authority: Arc::new(GroupConsumerCloseAuthority::new()),
            _not_sync: PhantomData,
        }
    }

    pub(crate) fn try_register(
        port: GroupConsumerPort,
        lifetime: Arc<dyn Send + Sync>,
        request: GroupConsumerRegistration,
    ) -> Result<Self, GroupConsumerRegistrationError> {
        let timing =
            ClassicGroupTiming::try_new(DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_REBALANCE_TIMEOUT_MS)
                .unwrap_or_else(|_| unreachable!("fixed classic-group timing is valid"));
        let heartbeat = ClassicHeartbeatPolicy::try_new(
            DEFAULT_HEARTBEAT_INTERVAL_TICKS,
            DEFAULT_HEARTBEAT_ATTEMPT_TIMEOUT_TICKS,
        )
        .unwrap_or_else(|_| unreachable!("fixed classic heartbeat policy is valid"));
        let rejoin = ClassicRejoinPolicy::try_new(
            DEFAULT_REJOIN_BACKOFF_TICKS,
            DEFAULT_REJOIN_ATTEMPT_TIMEOUT_TICKS,
        )
        .unwrap_or_else(|_| unreachable!("fixed classic rejoin policy is valid"));
        let (
            group,
            group_instance_id,
            topics,
            protocol,
            missing_offset_policy,
            read_isolation,
            processing_policy,
        ) = request.into_validated_parts().map_err(|request| {
            GroupConsumerRegistrationError::new(
                GroupConsumerRegistrationErrorKind::InvalidInput,
                request,
            )
        })?;
        match port.try_register_controlled(
            group,
            group_instance_id,
            topics,
            protocol,
            timing,
            heartbeat,
            rejoin,
            missing_offset_policy.into_core(),
            read_isolation.core(),
            processing_policy,
        ) {
            Ok(GroupConsumerPortRegistrationAccepted {
                group_id,
                close_authority,
            }) => Ok(Self {
                group_id,
                port,
                lifetime,
                close_authority,
                _not_sync: PhantomData,
            }),
            Err(failure) => {
                let kind = match failure.kind.public_category() {
                    GroupConsumerPortRegistrationCategory::Closed => {
                        GroupConsumerRegistrationErrorKind::Closed
                    }
                    GroupConsumerPortRegistrationCategory::Contended => {
                        GroupConsumerRegistrationErrorKind::Contended
                    }
                    GroupConsumerPortRegistrationCategory::Backpressure => {
                        GroupConsumerRegistrationErrorKind::Backpressure
                    }
                    GroupConsumerPortRegistrationCategory::InvalidInput => {
                        GroupConsumerRegistrationErrorKind::InvalidInput
                    }
                    GroupConsumerPortRegistrationCategory::InternalInvariant => {
                        GroupConsumerRegistrationErrorKind::Internal
                    }
                };
                let mut request =
                    GroupConsumerRegistration::new(failure.group, failure.local_topics)
                        .with_protocol(protocol);
                if let Some(group_instance_id) = failure.group_instance_id {
                    request = request.with_group_instance_id(group_instance_id);
                }
                let request = request
                    .with_missing_offset_policy(missing_offset_policy)
                    .with_read_isolation(read_isolation)
                    .with_processing_timeout(Duration::from_nanos(
                        processing_policy.timeout_ticks(),
                    ));
                Err(GroupConsumerRegistrationError::new(kind, request))
            }
        }
    }

    /// Begins one membership cycle under a deadline captured by this call.
    ///
    /// A returned error is pre-core and leaves this unique handle retryable.
    /// Advisory wake failure or an impossible post-core shape remains an
    /// accepted result because deterministic ownership already transferred.
    pub fn try_start(
        &mut self,
        timeout: Duration,
    ) -> Result<GroupConsumerStartAccepted, GroupConsumerStartError> {
        let capture = GroupConsumerStartCapture::capture(self.port.clone(), timeout)?;
        self.try_start_captured(capture)
    }

    /// Begins membership using a deadline captured before higher-layer input work.
    pub fn try_start_captured(
        &mut self,
        capture: GroupConsumerStartCapture,
    ) -> Result<GroupConsumerStartAccepted, GroupConsumerStartError> {
        capture.admit(&self.port, self.group_id).map(|accepted| {
            GroupConsumerStartAccepted::new(accepted.entry_faulted(), accepted.wake_failed())
        })
    }

    /// Immediately transfers one already-authorized group Fetch delivery.
    ///
    /// This observation takes fresh time only to renew the application
    /// processing lease. It starts no membership, Fetch, or public timeout.
    pub fn try_take_batch(
        &mut self,
    ) -> Result<Option<GroupConsumerBatch>, GroupConsumerTryTakeBatchError> {
        self.port
            .try_take_delivery(self.group_id)
            .map(|delivery| {
                delivery.map(|delivery| {
                    GroupConsumerBatch::new(delivery, self.port.clone(), Arc::clone(&self.lifetime))
                })
            })
            .map_err(|error| GroupConsumerTryTakeBatchError::from_port(&error))
    }

    #[cfg(test)]
    pub(crate) const fn group_id_for_test(&self) -> kafka_client_core::GroupId {
        self.group_id
    }
}
