//! Capture-after metadata-quorum voter-removal request tests.

use crate::admin::RaftVoterIdentity;

use super::request::RemoveRaftVoterAdminRequest;

#[test]
fn inert_request_keeps_facade_identity_unconverted_until_submission() {
    let directory_id = [0xa5; 16];
    let request = RemoveRaftVoterAdminRequest::new(RaftVoterIdentity::new(-7, directory_id))
        .with_cluster_id(String::from("cluster-b"));

    let (cluster_id, identity) = request.into_parts();
    assert_eq!(cluster_id.as_deref(), Some("cluster-b"));
    assert_eq!(identity.into_parts(), (-7, directory_id));
}
