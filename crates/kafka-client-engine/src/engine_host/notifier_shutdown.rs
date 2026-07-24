//! Linear collection and off-notifier joining of every completion notifier.

use crate::completion::{NotifierJoin, NotifierJoinError};

use super::EngineHostError;

/// Exact notifier-join owner carried only into the off-notifier finalizer.
pub(crate) struct NotifierShutdownOwner {
    notifiers: Vec<NotifierJoin>,
}

impl NotifierShutdownOwner {
    pub(super) const fn new(notifiers: Vec<NotifierJoin>) -> Self {
        Self { notifiers }
    }

    pub(super) fn join_off_notifier(&mut self) -> Result<(), NotifierJoinError> {
        let mut failure = None;
        while let Some(notifier) = self.notifiers.pop() {
            if let Err(error) = notifier.join_off_notifier() {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for NotifierShutdownOwner {
    fn drop(&mut self) {
        while let Some(notifier) = self.notifiers.pop() {
            let _join_result = notifier.join_off_notifier();
        }
    }
}

pub(super) fn collect_notification_joins(
    producer: NotifierJoin,
    domains: impl IntoIterator<Item = (Result<NotifierJoin, EngineHostError>, Option<NotifierJoin>)>,
) -> (Vec<NotifierJoin>, Option<EngineHostError>) {
    let mut notifiers = Vec::with_capacity(3);
    notifiers.push(producer);
    let mut failure: Option<EngineHostError> = None;
    for (domain, fallback) in domains {
        match domain {
            Ok(domain) => notifiers.push(domain),
            Err(error) => {
                if let Some(domain) = fallback {
                    notifiers.push(domain);
                }
                failure = Some(match failure {
                    Some(primary) => primary.with_cleanup(error),
                    None => error,
                });
            }
        }
    }
    (notifiers, failure)
}
