//! Shared policy configuration, traversal, and fixture support.

#![allow(dead_code, unused_imports)]

mod async_capability;
mod authority;
mod call_capability;
mod capability;
mod cargo_targets;
mod config;
mod duplication;
mod files;
mod github_yaml;
mod glob_import;
mod invocation;
mod invocation_pattern;
mod invocation_scope;
mod macro_scope;
mod macro_token;
mod method_capability;
mod ownership;
mod ownership_methods;
mod policy_path;
mod size_policy;
mod source_graph;
mod source_include;
mod test_evidence;
mod test_layout;

pub(crate) use authority::authority_token_violations;
pub(crate) use call_capability::call_capability_violations;
pub(crate) use capability::capability_violations;
pub(crate) use cargo_targets::{PackageTargets, package_targets, workspace_targets};
pub(crate) use config::{
    AuthorityToken, Budget, BudgetAllow, BudgetBaseline, CallCapabilityRule, CapabilityAllow,
    CapabilityRule, DependencyRule, FileBudgets, GuardConfig, LinearOwner, MethodCapabilityRule,
    MutationOwner, TestMirror, load_config, parse_config,
};
pub(crate) use duplication::{authority_linear_violations, linear_violations};
pub(crate) use files::{
    FileClass, WalkScope, classify, classify_with_package_roots, display_path, fixture_files,
    is_facade, is_integration_test, is_test_only_source, read, rust_files, rust_files_under,
    workspace_package_roots, workspace_root,
};
pub(crate) use github_yaml::{YamlNode, entry as yaml_entry, parse as parse_github_yaml};
pub(crate) use glob_import::glob_import_violations;
pub(crate) use invocation::{invocation_candidate_matches, invocation_matches, invocations};
pub(crate) use macro_scope::{MacroScope, source_capable_definition};
pub(crate) use macro_token::macro_identifiers;
pub(crate) use method_capability::method_capability_violations;
pub(crate) use ownership::mutation_violations;
pub(crate) use policy_path::valid_relative_policy_path;
pub(crate) use size_policy::size_violations;
pub(crate) use source_graph::workspace_source_violations;
pub(crate) use source_include::rust_source_expansion_violation;
pub(crate) use test_evidence::runnable_test_count;
pub(crate) use test_layout::{Declaration, declaration, is_unit_test, sibling_facade};
