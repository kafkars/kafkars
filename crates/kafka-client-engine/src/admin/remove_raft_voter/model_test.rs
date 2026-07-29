//! Capture-after metadata-quorum voter-removal model tests.

use super::RemoveRaftVoterRequest;

#[test]
fn inert_request_preserves_invalid_scalars_until_plan_conversion() {
    let request = RemoveRaftVoterRequest::new(Some(String::new()), -1, [0; 16]);

    assert_eq!(request.cluster_id(), Some(""));
    assert_eq!(request.voter_id(), -1);
    assert_eq!(request.voter_directory_id(), [0; 16]);
    assert!(request.into_plan().is_err());
}
