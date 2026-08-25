//! The normative semantic registry names live executable evidence.

use std::{collections::BTreeMap, fs, path::Path, path::PathBuf};

const ZRAIL_LEDGER_HEADER: &str = "existing_guard\tprotected_invariant\tzrail_rail\tgoverned_surface\tnegative_mutation\told_result\tnew_result\treplacement_scope\tdecision";

#[test]
fn semantic_invariant_registry_names_live_evidence() {
    let workspace = workspace_root();
    let violations = kafka_client_guardrails::check_invariant_registry(&workspace);

    assert!(
        violations.is_empty(),
        "semantic invariant registry violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn zrail_migration_ledger_accounts_for_every_guard_target() {
    let workspace = workspace_root();
    let ledger_path = workspace.join("docs/ZRAIL_GUARD_LEDGER.tsv");
    let ledger = fs::read_to_string(&ledger_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", ledger_path.display()));
    let mut lines = ledger.lines();

    assert_eq!(
        lines.next(),
        Some(ZRAIL_LEDGER_HEADER),
        "Zrail guard ledger header changed without updating its schema"
    );

    let mut listed_guards = Vec::new();
    let mut scope_counts = BTreeMap::new();
    for (offset, line) in lines.enumerate() {
        let line_number = offset + 2;
        let fields = line.split('\t').collect::<Vec<_>>();
        assert_eq!(
            fields.len(),
            9,
            "{}:{line_number} must contain exactly nine tab-separated fields",
            ledger_path.display()
        );
        assert!(
            fields.iter().all(|field| !field.trim().is_empty()),
            "{}:{line_number} contains an empty field",
            ledger_path.display()
        );

        let guard = fields[0];
        assert!(
            guard.ends_with("_guard.rs"),
            "{}:{line_number} names a non-guard target: {guard}",
            ledger_path.display()
        );
        listed_guards.push(guard.to_owned());

        let scope = fields[7];
        let expected_decision = match scope {
            "direct" => "retain-until-parity",
            "partial" => "retain-partial",
            "none" => "retain-semantic",
            other => panic!(
                "{}:{line_number} has unknown replacement scope {other}",
                ledger_path.display()
            ),
        };
        assert_eq!(
            fields[8],
            expected_decision,
            "{}:{line_number} decision is inconsistent with replacement scope",
            ledger_path.display()
        );
        *scope_counts.entry(scope).or_insert(0_usize) += 1;
    }

    assert!(
        listed_guards.windows(2).all(|pair| pair[0] < pair[1]),
        "Zrail guard ledger must be strictly ordered and duplicate-free"
    );

    let tests_dir = workspace.join("crates/kafka-client-guardrails/tests");
    let mut guard_targets = fs::read_dir(&tests_dir)
        .unwrap_or_else(|error| panic!("read {}: {error}", tests_dir.display()))
        .map(|entry| {
            entry
                .unwrap_or_else(|error| panic!("read entry under {}: {error}", tests_dir.display()))
                .path()
        })
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with("_guard.rs"))
        })
        .map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_else(|| panic!("guard filename is UTF-8"))
                .to_owned()
        })
        .collect::<Vec<_>>();
    guard_targets.sort();

    assert_eq!(
        listed_guards, guard_targets,
        "Zrail guard ledger drifted from the integration guard targets"
    );
    assert_eq!(scope_counts.get("direct"), Some(&18));
    assert_eq!(scope_counts.get("partial"), Some(&77));
    assert_eq!(scope_counts.get("none"), Some(&6));
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map_or_else(
            || panic!("guardrail crate must be below workspace root"),
            Path::to_path_buf,
        )
}
