//! Stable metadata-quorum voter endpoint tests.

use super::RaftVoterEndpoint;

#[test]
fn endpoint_preserves_listener_host_and_unsigned_port() {
    let endpoint = RaftVoterEndpoint::new("CONTROLLER", "controller-2.internal", u16::MAX);

    assert_eq!(endpoint.listener(), "CONTROLLER");
    assert_eq!(endpoint.host(), "controller-2.internal");
    assert_eq!(endpoint.port(), u16::MAX);
    assert_eq!(
        endpoint.into_parts(),
        (
            String::from("CONTROLLER"),
            String::from("controller-2.internal"),
            u16::MAX,
        )
    );
}
