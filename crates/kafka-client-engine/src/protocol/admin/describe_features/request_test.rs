//! Request-shape evidence for explicit feature description.

use super::describe_features_request;

#[test]
fn request_names_the_client_without_inventing_cluster_or_node_identity() {
    let request = describe_features_request();

    assert_eq!(request.client_software_name.as_str(), "kafka-client-rs");
    assert!(!request.client_software_version.is_empty());
    assert_eq!(request.cluster_id, None);
    assert_eq!(request.node_id, -1);
    assert!(request.unknown_tagged_fields.is_empty());
}
