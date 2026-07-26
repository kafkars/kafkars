//! Deliberate foreign use of broker-rejection interpreter calls.

fn steal<T>(entry: T, transition: T, rejection: T) {
    exact_broker_error(rejection);
    install_stage_rejection(entry, transition);
    install_heartbeat_rejection(entry, transition);
}
