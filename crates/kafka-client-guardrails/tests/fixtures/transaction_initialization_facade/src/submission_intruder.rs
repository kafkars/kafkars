//! Forbidden second direct engine submission owner.

fn steal<T, R>(engine: &T, request: R) {
    let capture = engine.capture_transactional_owner_initialization();
    capture.initialize_transactional_owner(request);
}
