//! Shared policy configuration, traversal, and fixture support.

#![allow(dead_code, unused_imports)]

mod authority;
mod call_capability;
mod capability;
mod config;
mod duplication;
mod files;
mod github_yaml;
mod glob_import;
mod invocation;
mod invocation_pattern;
mod invocation_scope;
mod macro_token;
mod method_capability;
mod ownership;
mod test_layout;

pub(crate) use authority::authority_token_violations;
pub(crate) use call_capability::call_capability_violations;
pub(crate) use capability::capability_violations;
pub(crate) use config::{
    AuthorityToken, Budget, BudgetAllow, BudgetBaseline, CallCapabilityRule, CapabilityRule,
    DependencyRule, FileBudgets, GuardConfig, LinearOwner, MethodCapabilityRule, MutationOwner,
    TestMirror, load_config, parse_config,
};
pub(crate) use duplication::{authority_linear_violations, linear_violations};
pub(crate) use files::{
    FileClass, WalkScope, classify, display_path, fixture_files, is_facade, is_test_only_source,
    read, rust_files, rust_files_under, workspace_root,
};
pub(crate) use github_yaml::{YamlNode, entry as yaml_entry, parse as parse_github_yaml};
pub(crate) use glob_import::glob_import_violations;
pub(crate) use invocation::{invocation_candidate_matches, invocation_matches, invocations};
pub(crate) use macro_token::macro_identifiers;
pub(crate) use method_capability::method_capability_violations;
pub(crate) use ownership::mutation_violations;
pub(crate) use test_layout::{Declaration, declaration, is_unit_test, sibling_facade};
