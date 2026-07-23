//! Ordered startup and rollback of the producer notification workers.

use crate::completion::{NotificationBudget, NotificationOwners};

use super::super::{
    ProducerHostStartError,
    host_error::PendingRecoveryStartupCleanupError,
    pending::{PendingNotificationRoute, PendingRecoveryStartupOwner},
};

pub(crate) fn start_notification_owners<T: Send + 'static, F>(
    notification_budget: NotificationBudget,
    pending_capacity: usize,
    start_notifier: F,
) -> Result<(NotificationOwners<T>, PendingNotificationRoute), ProducerHostStartError>
where
    F: FnOnce(NotificationBudget) -> std::io::Result<NotificationOwners<T>>,
{
    let mut route = PendingNotificationRoute::start(pending_capacity)
        .map_err(ProducerHostStartError::PendingRecovery)?;
    let owners = match start_notifier(notification_budget) {
        Ok(owners) => owners,
        Err(notifier) => {
            let cleanup = match route.begin_startup_rollback() {
                Some(stop) => {
                    let stop: PendingRecoveryStartupOwner = stop;
                    stop.finish_startup_rollback()
                        .err()
                        .map(PendingRecoveryStartupCleanupError::Join)
                }
                None => Some(PendingRecoveryStartupCleanupError::MissingJoin),
            };
            return match cleanup {
                Some(cleanup) => {
                    Err(ProducerHostStartError::NotificationRollback { notifier, cleanup })
                }
                None => Err(ProducerHostStartError::Notifier(notifier)),
            };
        }
    };
    Ok((owners, route))
}
