//! Shared fail-closed accessors for structurally parsed GitHub YAML.

use crate::support::{YamlNode, parse_github_yaml, yaml_entry};

pub(super) type Mapping = [(String, YamlNode)];

pub(super) fn document(
    source: &str,
    label: &str,
    violations: &mut Vec<String>,
) -> Option<YamlNode> {
    match parse_github_yaml(source) {
        Ok(document) => Some(document),
        Err(error) => {
            violations.push(format!(
                "{label} is outside the supported YAML shape: {error}"
            ));
            None
        }
    }
}

pub(super) fn mapping<'a>(
    node: &'a YamlNode,
    label: &str,
    violations: &mut Vec<String>,
) -> Option<&'a Mapping> {
    if let Some(mapping) = node.mapping() {
        Some(mapping)
    } else {
        violations.push(format!("{label} must be a YAML mapping"));
        None
    }
}

pub(super) fn child_mapping<'a>(
    parent: &'a Mapping,
    key: &str,
    label: &str,
    violations: &mut Vec<String>,
) -> Option<&'a Mapping> {
    let Some(node) = yaml_entry(parent, key) else {
        violations.push(format!("{label} is missing"));
        return None;
    };
    mapping(node, label, violations)
}

pub(super) fn child_sequence<'a>(
    parent: &'a Mapping,
    key: &str,
    label: &str,
    violations: &mut Vec<String>,
) -> Option<&'a [YamlNode]> {
    let Some(node) = yaml_entry(parent, key) else {
        violations.push(format!("{label} is missing"));
        return None;
    };
    if let Some(sequence) = node.sequence() {
        Some(sequence)
    } else {
        violations.push(format!("{label} must be a YAML sequence"));
        None
    }
}

pub(super) fn scalar<'a>(mapping: &'a Mapping, key: &str) -> Option<&'a str> {
    yaml_entry(mapping, key).and_then(YamlNode::scalar)
}

pub(super) fn block<'a>(mapping: &'a Mapping, key: &str) -> Option<&'a [String]> {
    yaml_entry(mapping, key).and_then(YamlNode::block)
}

pub(super) fn reject_bypass(mapping: &Mapping, label: &str, violations: &mut Vec<String>) {
    if yaml_entry(mapping, "if").is_some() {
        violations.push(format!("{label} may not be conditional"));
    }
    if yaml_entry(mapping, "continue-on-error").is_some() {
        violations.push(format!("{label} may not continue on error"));
    }
}

pub(super) fn reject_run_bypass(mapping: &Mapping, label: &str, violations: &mut Vec<String>) {
    reject_bypass(mapping, label, violations);
    if yaml_entry(mapping, "shell").is_some() {
        violations.push(format!("{label} may not override its runner shell"));
    }
    if yaml_entry(mapping, "working-directory").is_some() {
        violations.push(format!("{label} may not override its working directory"));
    }
    if yaml_entry(mapping, "env").is_some() {
        violations.push(format!("{label} may not override its environment"));
    }
}

pub(super) fn exact_scalar(mapping: &Mapping, key: &str, expected: &str) -> bool {
    scalar(mapping, key) == Some(expected)
}

pub(super) fn reject_unexpected_keys(
    mapping: &Mapping,
    allowed: &[&str],
    label: &str,
    violations: &mut Vec<String>,
) {
    for (key, _) in mapping {
        if !allowed.contains(&key.as_str()) {
            violations.push(format!("{label} contains unsupported key `{key}`"));
        }
    }
}
