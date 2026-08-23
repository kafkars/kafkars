//! Atomic registration and membership-start admission under one shard lock.

use std::sync::Arc;

use crate::{EngineShareConsumerFetchConfig, clock::DeadlineCapture};

use super::port::{ShareConsumerPort, ShareRegistrationAdmission, ShareRegistrationPortFailure};

impl ShareConsumerPort {
    pub(crate) fn try_register(
        &self,
        group: Arc<str>,
        rack: Option<Arc<str>>,
        topics: Vec<Arc<str>>,
        fetch: EngineShareConsumerFetchConfig,
    ) -> Result<ShareRegistrationAdmission, ShareRegistrationPortFailure> {
        if self.shared.admission_is_closed() {
            return Err(ShareRegistrationPortFailure::closed(
                group, rack, topics, fetch,
            ));
        }
        let mut registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(kind) => {
                return Err(ShareRegistrationPortFailure::lock(
                    kind, group, rack, topics, fetch,
                ));
            }
        };
        if self.shared.admission_is_closed() {
            return Err(ShareRegistrationPortFailure::closed(
                group, rack, topics, fetch,
            ));
        }
        let group_id = registry
            .try_register(group, rack, topics, fetch)
            .map_err(ShareRegistrationPortFailure::registry)?;
        drop(registry);
        Ok(ShareRegistrationAdmission {
            group_id,
            wake: self.shared.request_turn().err(),
        })
    }

    pub(super) fn try_register_started(
        &self,
        group: Arc<str>,
        rack: Option<Arc<str>>,
        topics: Vec<Arc<str>>,
        fetch: EngineShareConsumerFetchConfig,
        capture: DeadlineCapture,
    ) -> Result<ShareRegistrationAdmission, ShareRegistrationPortFailure> {
        if self.shared.admission_is_closed() {
            return Err(ShareRegistrationPortFailure::closed(
                group, rack, topics, fetch,
            ));
        }
        let mut registry = match self.shared.try_registry() {
            Ok(registry) => registry,
            Err(kind) => {
                return Err(ShareRegistrationPortFailure::lock(
                    kind, group, rack, topics, fetch,
                ));
            }
        };
        if self.shared.admission_is_closed() {
            return Err(ShareRegistrationPortFailure::closed(
                group, rack, topics, fetch,
            ));
        }
        let group_id = registry
            .try_register(group, rack, topics, fetch)
            .map_err(ShareRegistrationPortFailure::registry)?;
        registry
            .try_begin(group_id, capture)
            .unwrap_or_else(|_error| unreachable!("fresh share registration begins exactly once"));
        drop(registry);
        Ok(ShareRegistrationAdmission {
            group_id,
            wake: self.shared.request_turn().err(),
        })
    }
}
