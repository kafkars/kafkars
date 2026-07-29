//! Capture-after metadata-quorum voter-addition request tests.

use crate::admin::{RaftVoterEndpoint, RaftVoterIdentity};

use super::request::AddRaftVoterAdminRequest;

#[test]
fn inert_request_keeps_facade_values_unconverted_until_submission() {
    let directory_id = [0x5a; 16];
    let request = AddRaftVoterAdminRequest::new(
        RaftVoterIdentity::new(-3, directory_id),
        vec![RaftVoterEndpoint::new(
            "CONTROLLER",
            "controller.internal",
            9093,
        )],
    )
    .with_cluster_id(String::from("cluster-a"));

    let (cluster_id, identity, endpoints) = request.into_parts();
    assert_eq!(cluster_id.as_deref(), Some("cluster-a"));
    assert_eq!(identity.into_parts(), (-3, directory_id));
    assert_eq!(
        endpoints
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("endpoint expected"))
            .into_parts(),
        (
            String::from("CONTROLLER"),
            String::from("controller.internal"),
            9093,
        )
    );
}
