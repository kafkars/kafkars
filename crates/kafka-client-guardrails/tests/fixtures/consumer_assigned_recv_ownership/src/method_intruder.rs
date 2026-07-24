//! Deliberately steals the non-terminal notification-port factory.

fn violate(notifier: Notifier) {
    let _port = notifier.notification_port(Ticket::Recv);
}
