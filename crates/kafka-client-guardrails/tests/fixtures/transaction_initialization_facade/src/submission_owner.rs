//! Sole allowed private bridge submission owner.

fn submit<T, R>(engine: &T, request: R) {
    let capture = engine.capture_transactional_owner_initialization();
    capture.initialize_transactional_owner(request);
}
