//! Lossless registration recovery and immutable `ShareFetch` configuration access.

use std::sync::Arc;

use crate::EngineShareConsumerFetchConfig;

use super::ShareConsumerEntry;

pub(in crate::consumer::share) struct ShareRegistrationParts {
    pub(in crate::consumer::share) group: Arc<str>,
    pub(in crate::consumer::share) rack: Option<Arc<str>>,
    pub(in crate::consumer::share) topics: Vec<Arc<str>>,
    pub(in crate::consumer::share) fetch: EngineShareConsumerFetchConfig,
}

impl ShareConsumerEntry {
    pub(in crate::consumer::share) const fn fetch_config(&self) -> EngineShareConsumerFetchConfig {
        self.fetch_config
    }

    pub(in crate::consumer::share) fn into_registration_parts(self) -> ShareRegistrationParts {
        let Self {
            group,
            rack,
            topics,
            fetch_config,
            member,
            resolved_topics,
            start,
            membership,
            topic_call,
            heartbeat_call,
            fetch,
            fault,
            close,
            ..
        } = self;
        drop((
            member,
            resolved_topics,
            start,
            membership,
            topic_call,
            heartbeat_call,
            fetch,
            fault,
            close,
        ));
        ShareRegistrationParts {
            group,
            rack,
            topics,
            fetch: fetch_config,
        }
    }
}
