//! Protected constructor calls stay with their configured authority owners.

mod support;

use support::{CallCapabilityRule, call_capability_violations, fixture_files};

const PROTECTED_CALL: &str = "PendingNotificationPermitPool::from_pending_permit_authority";

#[test]
fn constructor_call_allowlist_has_positive_and_negative_evidence() {
    let root = fixture_files("unbounded_channel").0;
    let allowed = [CallCapabilityRule {
        root: "src".to_owned(),
        call: PROTECTED_CALL.to_owned(),
        allowed_paths: vec![
            "src/at_shadow_negative.rs".to_owned(),
            "src/chained_use_alias.rs".to_owned(),
            "src/constructor_call.rs".to_owned(),
            "src/function_item_alias.rs".to_owned(),
            "src/glob_renamed_reexport.rs".to_owned(),
            "src/identity_wrapper.rs".to_owned(),
            "src/local_shadow_negative.rs".to_owned(),
            "src/parameter_shadow_negative.rs".to_owned(),
            "src/renamed_constructor.rs".to_owned(),
            "src/self_constructor.rs".to_owned(),
            "src/type_alias.rs".to_owned(),
            "src/typed_cast_reference_deref.rs".to_owned(),
        ],
    }];
    assert!(
        call_capability_violations(&root, &allowed).is_empty(),
        "configured constructor owner should be accepted"
    );

    let forbidden = [CallCapabilityRule {
        root: "src".to_owned(),
        call: PROTECTED_CALL.to_owned(),
        allowed_paths: Vec::new(),
    }];
    let violations = call_capability_violations(&root, &forbidden);
    assert!(
        [
            "at_shadow_negative.rs",
            "chained_use_alias.rs",
            "constructor_call.rs",
            "function_item_alias.rs",
            "glob_renamed_reexport.rs",
            "identity_wrapper.rs",
            "local_shadow_negative.rs",
            "parameter_shadow_negative.rs",
            "renamed_constructor.rs",
            "self_constructor.rs",
            "type_alias.rs",
            "typed_cast_reference_deref.rs",
        ]
        .into_iter()
        .all(|fixture| violations.iter().any(|value| value.contains(fixture))),
        "constructor allowlist missed a direct or aliased bypass: {violations:?}"
    );
    assert!(
        [
            "at_shadow_positive.rs",
            "local_shadow_positive.rs",
            "parameter_shadow_positive.rs",
            "self_constructor_positive.rs",
        ]
        .into_iter()
        .all(|fixture| !violations.iter().any(|value| value.contains(fixture))),
        "constructor allowlist leaked through a scoped shadow: {violations:?}"
    );

    let shadow_only = [CallCapabilityRule {
        root: "src".to_owned(),
        call: PROTECTED_CALL.to_owned(),
        allowed_paths: vec![
            "src/at_shadow_positive.rs".to_owned(),
            "src/local_shadow_positive.rs".to_owned(),
            "src/parameter_shadow_positive.rs".to_owned(),
        ],
    }];
    let shadow_violations = call_capability_violations(&root, &shadow_only);
    assert!(
        [
            "at_shadow_positive.rs",
            "local_shadow_positive.rs",
            "parameter_shadow_positive.rs",
        ]
        .into_iter()
        .all(|fixture| shadow_violations.iter().any(|value| {
            value.contains(fixture) && value.contains("decorative call capability path")
        })),
        "shadow paths falsely satisfied an allowlist: {shadow_violations:?}"
    );
}

#[test]
fn cyclic_alias_resolution_fails_closed() {
    let root = fixture_files("invocation_resolution_cycle").0;
    let violations = call_capability_violations(
        &root,
        &[CallCapabilityRule {
            root: "src".to_owned(),
            call: PROTECTED_CALL.to_owned(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(violations.iter().any(
        |value| value.contains("cycle.rs") && value.contains("outside its complete allowlist")
    ));
}

#[test]
fn renamed_cross_file_reexports_cannot_hide_protected_constructors() {
    let root = fixture_files("protected_reexport").0;
    let violations = call_capability_violations(
        &root,
        &[CallCapabilityRule {
            root: "src".to_owned(),
            call: PROTECTED_CALL.to_owned(),
            allowed_paths: vec!["src/owner.rs".to_owned()],
        }],
    );
    assert!(
        ["public_consumer.rs", "private_consumer.rs"]
            .into_iter()
            .all(|fixture| violations.iter().any(|value| value.contains(fixture))),
        "renamed cross-file re-export escaped: {violations:?}"
    );
    assert!(
        !violations.iter().any(|value| value.contains("positive.rs")),
        "unrelated method was treated as protected: {violations:?}"
    );
    assert!(
        !violations
            .iter()
            .any(|value| value.contains("decorative call capability")),
        "direct owner call did not satisfy its allowlist: {violations:?}"
    );
}

#[test]
fn macro_tokens_cannot_hide_protected_constructors() {
    let root = fixture_files("protected_macro").0;
    let violations = call_capability_violations(
        &root,
        &[CallCapabilityRule {
            root: "src".to_owned(),
            call: PROTECTED_CALL.to_owned(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(
        violations
            .iter()
            .any(|value| value.contains("inside a macro")),
        "protected macro token escaped: {violations:?}"
    );
}
