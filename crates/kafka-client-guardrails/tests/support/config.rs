//! Strict deserialization for the repository guardrail policy.

use std::fs;
use std::path::Path;

use serde::Deserialize;

/// Complete checked-in guardrail policy.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GuardConfig {
    pub(crate) schema: u32,
    pub(crate) paths: Paths,
    pub(crate) budgets: FileBudgets,
    pub(crate) test_mirrors: Vec<TestMirror>,
    pub(crate) dependency_rules: Vec<DependencyRule>,
    pub(crate) capability_rules: Vec<CapabilityRule>,
    pub(crate) mutation_owners: Vec<MutationOwner>,
    pub(crate) linear_owners: Vec<LinearOwner>,
}

/// Source roots included in and excluded from live inspection.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Paths {
    pub(crate) rust_roots: Vec<String>,
    pub(crate) excluded_roots: Vec<String>,
}

/// Role-specific limits plus reviewed exceptions.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileBudgets {
    pub(crate) facade: Budget,
    pub(crate) implementation: Budget,
    pub(crate) test: Budget,
    pub(crate) auxiliary: Budget,
    pub(crate) baseline: Vec<BudgetBaseline>,
    pub(crate) allow: Vec<BudgetAllow>,
}

/// Design target, attention threshold, and absolute ceiling.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Budget {
    pub(crate) target: usize,
    pub(crate) soft: usize,
    pub(crate) hard: usize,
}

/// Reviewed file frozen above its role's target.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BudgetBaseline {
    pub(crate) path: String,
    pub(crate) lines: usize,
    pub(crate) reason: String,
}

/// Exceptional file permitted above its role's absolute ceiling.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BudgetAllow {
    pub(crate) path: String,
    pub(crate) reason: String,
    pub(crate) owner: String,
    pub(crate) issue: String,
}

/// A load-bearing production module and its sibling unit-test module.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TestMirror {
    pub(crate) production: String,
    pub(crate) test: String,
}

/// Exact dependency allowlist for one workspace package.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DependencyRule {
    pub(crate) package: String,
    pub(crate) allowed_internal: Vec<String>,
    pub(crate) allowed_external: Vec<String>,
}

/// Source capability tokens forbidden beneath one root.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapabilityRule {
    pub(crate) root: String,
    pub(crate) forbidden: Vec<String>,
}

/// One load-bearing field and the modules permitted to mutate it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MutationOwner {
    pub(crate) owner_type: String,
    pub(crate) field: String,
    pub(crate) allowed_paths: Vec<String>,
}

/// One lifecycle owner that must remain nonduplicable.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LinearOwner {
    pub(crate) owner_type: String,
    pub(crate) path: String,
}

const SUPPORTED_SCHEMA: u32 = 1;

pub(crate) fn load_config(workspace: &Path) -> GuardConfig {
    let path = workspace.join("guardrails.toml");
    let source = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let config =
        parse_config(&source).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    assert_eq!(
        config.schema,
        SUPPORTED_SCHEMA,
        "{} declares unsupported policy schema {}",
        path.display(),
        config.schema
    );
    config
}

pub(crate) fn parse_config(source: &str) -> Result<GuardConfig, toml::de::Error> {
    toml::from_str(source)
}
