//! Exact return-ownership ratchet for rejected assigned-consumer builds.

mod support;

use support::{LinearOwner, fixture_files, linear_violations, load_config, workspace_root};

const BUILD_ERROR: &str = "crates/kafkars/src/consumer/assigned_build_error.rs";
const BUILD_ERROR_TEST: &str = "crates/kafkars/src/consumer/assigned_build_error_test.rs";

#[test]
fn checked_in_build_rejection_is_linear_and_sibling_tested() {
    let config = load_config(&workspace_root());
    let mirrors = config
        .test_mirrors
        .iter()
        .filter(|rule| rule.production == BUILD_ERROR)
        .collect::<Vec<_>>();
    assert_eq!(mirrors.len(), 1, "{BUILD_ERROR} needs one test mirror");
    assert_eq!(mirrors[0].test, BUILD_ERROR_TEST);

    let owners = config
        .linear_owners
        .iter()
        .filter(|rule| rule.owner_type == "AssignedConsumerBuildError" && rule.path == BUILD_ERROR)
        .collect::<Vec<_>>();
    assert_eq!(
        owners.len(),
        1,
        "AssignedConsumerBuildError needs one exact linear rule"
    );
}

#[test]
fn fixture_rejects_clone_and_copy_for_the_returned_builder_owner() {
    let (root, files) = fixture_files("consumer_assigned_build_ownership");
    let violations = linear_violations(
        &root,
        &files,
        &[LinearOwner {
            owner_type: "AssignedConsumerBuildError".to_owned(),
            path: "src/linear_intruder.rs".to_owned(),
        }],
    );

    for derived in ["derives Clone", "derives Copy"] {
        assert!(violations.iter().any(|violation| {
            violation.contains("AssignedConsumerBuildError") && violation.contains(derived)
        }));
    }
}
