//! Stable workspace versions cannot retain prerelease build or runtime dependencies.

use toml::Value;

const RELEASE_DEPENDENCY_SECTIONS: [&str; 2] = ["dependencies", "build-dependencies"];

pub(crate) fn release_dependency_violations(
    root_source: &str,
    package_sources: &[(&str, &str)],
) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(root) = parse("workspace", root_source, &mut violations) else {
        return violations;
    };
    let Some(version) = root
        .get("workspace")
        .and_then(|workspace| workspace.get("package"))
        .and_then(|package| package.get("version"))
        .and_then(Value::as_str)
    else {
        violations.push("workspace package version must be a string".to_owned());
        return violations;
    };
    if requirement_has_prerelease(version) {
        return violations;
    }

    let workspace_dependencies = root
        .get("workspace")
        .and_then(|workspace| workspace.get("dependencies"))
        .and_then(Value::as_table);
    for (label, source) in package_sources {
        let Some(package) = parse(label, source, &mut violations) else {
            continue;
        };
        if is_publishable(&package) {
            inspect_dependencies(label, &package, workspace_dependencies, &mut violations);
        }
    }
    violations
}

fn parse(label: &str, source: &str, violations: &mut Vec<String>) -> Option<Value> {
    match source.parse::<Value>() {
        Ok(value) => Some(value),
        Err(error) => {
            violations.push(format!("{label} manifest is not TOML: {error}"));
            None
        }
    }
}

fn is_publishable(manifest: &Value) -> bool {
    manifest
        .get("package")
        .and_then(|package| package.get("publish"))
        .and_then(Value::as_bool)
        != Some(false)
}

fn inspect_dependencies(
    label: &str,
    manifest: &Value,
    workspace_dependencies: Option<&toml::map::Map<String, Value>>,
    violations: &mut Vec<String>,
) {
    for section in RELEASE_DEPENDENCY_SECTIONS {
        inspect_section(
            label,
            section,
            manifest.get(section),
            workspace_dependencies,
            violations,
        );
    }
    let Some(targets) = manifest.get("target").and_then(Value::as_table) else {
        return;
    };
    for (target, specification) in targets {
        for section in RELEASE_DEPENDENCY_SECTIONS {
            inspect_section(
                label,
                &format!("target {target} {section}"),
                specification.get(section),
                workspace_dependencies,
                violations,
            );
        }
    }
}

fn inspect_section(
    label: &str,
    section: &str,
    value: Option<&Value>,
    workspace_dependencies: Option<&toml::map::Map<String, Value>>,
    violations: &mut Vec<String>,
) {
    let Some(dependencies) = value.and_then(Value::as_table) else {
        return;
    };
    for (name, specification) in dependencies {
        let requirement = if uses_workspace(specification) {
            workspace_dependencies
                .and_then(|dependencies| dependencies.get(name))
                .and_then(direct_requirement)
        } else {
            direct_requirement(specification)
        };
        if requirement.is_some_and(requirement_has_prerelease) {
            violations.push(format!(
                "stable {label} {section} dependency {name} may not require a prerelease"
            ));
        }
    }
}

fn uses_workspace(specification: &Value) -> bool {
    specification.get("workspace").and_then(Value::as_bool) == Some(true)
}

fn direct_requirement(specification: &Value) -> Option<&str> {
    specification
        .as_str()
        .or_else(|| specification.get("version").and_then(Value::as_str))
}

fn requirement_has_prerelease(requirement: &str) -> bool {
    requirement
        .split(|character: char| character == ',' || character == '|' || character.is_whitespace())
        .map(|token| token.trim_start_matches(['=', '^', '~', '>', '<']))
        .filter_map(|token| token.split_once('-'))
        .any(|(release, prerelease)| {
            !prerelease.is_empty()
                && release.contains('.')
                && release.split('.').all(|component| {
                    !component.is_empty() && component.chars().all(|c| c.is_ascii_digit())
                })
        })
}
