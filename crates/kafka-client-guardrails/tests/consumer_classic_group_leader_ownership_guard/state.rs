//! Exact state-family evidence for linear partition-count execution.

use std::collections::BTreeSet;

use super::{
    expectations::{COUNT_STATES, STATE},
    support::{fixture_files, workspace_root},
};

#[test]
fn checked_in_partition_count_states_are_closed_and_exact() {
    assert_eq!(
        partition_count_states(&workspace_root().join(STATE)),
        expected_states()
    );
}

#[test]
fn fixture_rejects_an_unowned_partition_count_state() {
    let (root, _) = fixture_files("consumer_classic_group_leader_ownership");
    assert_ne!(
        partition_count_states(&root.join("src/state_intruder.rs")),
        expected_states()
    );
}

fn partition_count_states(path: &std::path::Path) -> BTreeSet<String> {
    let source = std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let syntax = syn::parse_file(&source)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    syntax
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Enum(item) if item.ident == "ClassicGroupExecutionState" => Some(
                item.variants
                    .iter()
                    .map(|variant| variant.ident.to_string())
                    .filter(|variant| variant.contains("PartitionCount"))
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{} has no ClassicGroupExecutionState", path.display()))
}

fn expected_states() -> BTreeSet<String> {
    COUNT_STATES
        .iter()
        .map(|variant| (*variant).to_owned())
        .collect()
}
