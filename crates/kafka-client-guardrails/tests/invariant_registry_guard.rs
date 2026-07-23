//! The normative semantic registry names live executable evidence.

use std::path::Path;

#[test]
fn semantic_invariant_registry_names_live_evidence() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| panic!("guardrail crate must be below workspace root"));
    let violations = kafka_client_guardrails::check_invariant_registry(workspace);

    assert!(
        violations.is_empty(),
        "semantic invariant registry violations:\n{}",
        violations.join("\n")
    );
}
