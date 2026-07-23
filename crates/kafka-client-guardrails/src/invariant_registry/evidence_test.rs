//! Negative fixtures for exact Rust `#[test]` evidence resolution.

use std::path::{Path, PathBuf};

use super::evidence;

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/invariant_registry")
}

#[test]
fn only_one_exact_test_item_is_accepted_as_evidence() {
    let root = fixture();
    assert!(evidence::violations(&root, "src/evidence_test.rs::actual_test").is_empty());

    for (reference, expected) in [
        ("src/evidence_test.rs", "must be"),
        ("../evidence_test.rs::actual_test", "not canonical"),
        ("src/evidence.rs::actual_test", "does not name"),
        ("src/missing_test.rs::actual_test", "cannot be read"),
        ("src/evidence_test.rs::missing", "names no function"),
        ("src/evidence_test.rs::ordinary", "ordinary function"),
        ("src/evidence_test.rs::ambiguous", "ambiguous"),
        ("src/invalid_test.rs::broken", "invalid Rust"),
    ] {
        let violations = evidence::violations(&root, reference);
        assert!(
            violations.iter().any(|value| value.contains(expected)),
            "evidence detector accepted {reference}: {violations:?}"
        );
    }
}
