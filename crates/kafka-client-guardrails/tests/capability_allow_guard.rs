//! Exact capability exceptions stay narrow, exercised, and justified.

mod support;

use support::{CapabilityAllow, CapabilityRule, capability_violations, fixture_files};

#[test]
fn exact_exercised_allow_does_not_exempt_sibling_sources() {
    let (root, _) = fixture_files("forbidden_capability");
    let rules = [CapabilityRule {
        root: "src".to_owned(),
        forbidden: vec!["std::net".to_owned()],
        allow: vec![CapabilityAllow {
            path: "src/alias.rs".to_owned(),
            capability: "std::net".to_owned(),
            reason: "fixture owns this one socket type".to_owned(),
        }],
    }];

    let violations = capability_violations(&root, &rules);

    assert!(!violations.iter().any(|value| value.contains("alias.rs")));
    assert!(
        violations
            .iter()
            .any(|value| value.contains("scoped_negative.rs")),
        "an exact allow must not exempt a sibling source: {violations:?}"
    );
}

#[test]
fn decorative_allow_is_rejected() {
    let (root, _) = fixture_files("forbidden_capability");
    let rules = [CapabilityRule {
        root: "src".to_owned(),
        forbidden: vec!["std::net".to_owned()],
        allow: vec![CapabilityAllow {
            path: "src/scoped_positive.rs".to_owned(),
            capability: "std::net".to_owned(),
            reason: "deliberately stale fixture".to_owned(),
        }],
    }];

    let violations = capability_violations(&root, &rules);

    assert!(
        violations
            .iter()
            .any(|value| value.contains("decorative capability allow")),
        "an unused exception must fail closed: {violations:?}"
    );
}
