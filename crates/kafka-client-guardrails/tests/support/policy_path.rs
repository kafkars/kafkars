//! Canonical repository-relative paths for reviewed guardrail policy entries.

use std::path::{Component, Path};

pub(crate) fn valid_relative_policy_path(value: &str) -> bool {
    if value.is_empty() || value.contains('\\') {
        return false;
    }
    let normalized = Path::new(value)
        .components()
        .map(|component| match component {
            Component::Normal(value) => value.to_str(),
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => None,
        })
        .collect::<Option<Vec<_>>>()
        .map(|components| components.join("/"));
    normalized.as_deref() == Some(value)
}
