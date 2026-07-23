//! Shared policy configuration, traversal, and fixture support.

#![allow(dead_code, unused_imports)]

mod config;
mod files;
mod ownership;

pub(crate) use config::{
    Budget, BudgetAllow, BudgetBaseline, CapabilityRule, DependencyRule, FileBudgets, GuardConfig,
    LinearOwner, MutationOwner, load_config, parse_config,
};
pub(crate) use files::{
    FileClass, WalkScope, classify, display_path, fixture_files, is_facade, read, rust_files,
    rust_files_under, workspace_root,
};
pub(crate) use ownership::{linear_violations, mutation_violations};
