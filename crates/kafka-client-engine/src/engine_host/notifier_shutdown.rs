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
    admin: Result<NotifierJoin, EngineHostError>,
    admin_fallback: Option<NotifierJoin>,
) -> (Vec<NotifierJoin>, Option<EngineHostError>) {
    let mut notifiers = Vec::with_capacity(2);
    notifiers.push(producer);
    match admin {
        Ok(admin) => {
            notifiers.push(admin);
            (notifiers, None)
        }
        Err(error) => {
            if let Some(admin) = admin_fallback {
                notifiers.push(admin);
            }
            (notifiers, Some(error))
        }
    }
}
