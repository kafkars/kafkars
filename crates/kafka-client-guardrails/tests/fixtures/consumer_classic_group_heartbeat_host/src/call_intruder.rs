//! Deliberate foreign construction and translation of hosted Heartbeat owners.

fn steal<T>(value: T) {
    classic_heartbeat_request_with_instance(value, value, value, value);
    interpret_heartbeat(value, value, value);
    normalize_classic_heartbeat_response(value, value);
}
