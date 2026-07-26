//! Configured owner proving broker-rejection interpreter calls are exact.

fn execute<T>(entry: T, transition: T, rejection: T) {
    exact_broker_error(rejection);
    install_stage_rejection(entry, transition);
    install_heartbeat_rejection(entry, transition);
}
