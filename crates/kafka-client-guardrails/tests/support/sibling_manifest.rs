//! Exact Cargo manifest contracts for reviewed sibling path dependencies.

use toml::Value;

const DEPENDENCY_SECTIONS: [&str; 3] = ["dependencies", "dev-dependencies", "build-dependencies"];

#[derive(Clone, Copy)]
struct Sibling {
    name: &'static str,
    path: &'static str,
}

const SIBLINGS: [Sibling; 4] = [
    Sibling {
        name: "kafka-driver",
        path: "../kafka-driver",
    },
    Sibling {
        name: "kafka-wire",
        path: "../kafka-protocol/crates/kafka-wire",
    },
    Sibling {
        name: "kafka-wire-records",
        path: "../kafka-protocol/crates/kafka-wire-records",
    },
    Sibling {
        name: "kafka-wire-core",
        path: "../kafka-protocol/crates/kafka-wire-core",
    },
];

pub(crate) fn violations(root_source: &str, engine_source: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let root = parse("workspace", root_source, &mut violations);
    let engine = parse("engine", engine_source, &mut violations);
    if let Some(root) = root {
        inspect_root(&root, &mut violations);
    }
    if let Some(engine) = engine {
        inspect_engine(&engine, &mut violations);
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

fn inspect_root(root: &Value, violations: &mut Vec<String>) {
    reject_overrides("workspace", root, violations);
    let workspace_dependencies = root
        .get("workspace")
        .and_then(|workspace| workspace.get("dependencies"))
        .and_then(Value::as_table);

    for sibling in SIBLINGS {
        let specification =
            workspace_dependencies.and_then(|dependencies| dependencies.get(sibling.name));
        if !is_exact_path(specification, sibling.path) {
            violations.push(format!(
                "workspace dependency {} must be exactly {{ path = \"{}\" }}",
                sibling.name, sibling.path
            ));
        }
    }
    if let Some(dependencies) = workspace_dependencies {
        reject_aliases("workspace dependencies", dependencies, violations);
    }
    reject_top_level_siblings("workspace root", root, violations);
    reject_target_siblings("workspace root", root, violations);
}

fn inspect_engine(engine: &Value, violations: &mut Vec<String>) {
    reject_overrides("engine", engine, violations);
    let dependencies = engine.get("dependencies").and_then(Value::as_table);
    for sibling in SIBLINGS {
        let specification = dependencies.and_then(|values| values.get(sibling.name));
        if !is_exact_workspace(specification) {
            violations.push(format!(
                "engine dependency {} must be exactly {{ workspace = true }}",
                sibling.name
            ));
        }
    }
    if let Some(dependencies) = dependencies {
        reject_aliases("engine dependencies", dependencies, violations);
    }
    if let Some(dependencies) = engine.get("dev-dependencies").and_then(Value::as_table) {
        reject_siblings("engine dev-dependencies", dependencies, violations);
    }
    if let Some(dependencies) = engine.get("build-dependencies").and_then(Value::as_table) {
        reject_siblings("engine build-dependencies", dependencies, violations);
    }
    reject_target_siblings("engine", engine, violations);
}

fn reject_overrides(label: &str, manifest: &Value, violations: &mut Vec<String>) {
    for section in ["patch", "replace"] {
        if manifest.get(section).is_some() {
            violations.push(format!("{label} manifest may not declare [{section}]"));
        }
    }
}

fn reject_top_level_siblings(label: &str, manifest: &Value, violations: &mut Vec<String>) {
    for section in DEPENDENCY_SECTIONS {
        if let Some(dependencies) = manifest.get(section).and_then(Value::as_table) {
            reject_siblings(&format!("{label} {section}"), dependencies, violations);
        }
    }
}

fn reject_target_siblings(label: &str, manifest: &Value, violations: &mut Vec<String>) {
    let Some(targets) = manifest.get("target").and_then(Value::as_table) else {
        return;
    };
    for (selector, target) in targets {
        for section in DEPENDENCY_SECTIONS {
            if let Some(dependencies) = target.get(section).and_then(Value::as_table) {
                reject_siblings(
                    &format!("{label} target {selector} {section}"),
                    dependencies,
                    violations,
                );
            }
        }
    }
}

fn reject_aliases(
    label: &str,
    dependencies: &toml::map::Map<String, Value>,
    violations: &mut Vec<String>,
) {
    for (declared, specification) in dependencies {
        let package = package_name(declared, specification);
        if is_sibling(package) && declared != package {
            violations.push(format!(
                "{label} aliases reviewed package {package} as {declared}"
            ));
        }
    }
}

fn reject_siblings(
    label: &str,
    dependencies: &toml::map::Map<String, Value>,
    violations: &mut Vec<String>,
) {
    for (declared, specification) in dependencies {
        let package = package_name(declared, specification);
        if is_sibling(package) {
            violations.push(format!(
                "{label} redeclares reviewed package {package} as {declared}"
            ));
        }
    }
}

fn package_name<'a>(declared: &'a str, specification: &'a Value) -> &'a str {
    specification
        .get("package")
        .and_then(Value::as_str)
        .unwrap_or(declared)
}

fn is_sibling(name: &str) -> bool {
    SIBLINGS.iter().any(|sibling| sibling.name == name)
}

fn is_exact_path(specification: Option<&Value>, expected: &str) -> bool {
    specification
        .and_then(Value::as_table)
        .is_some_and(|table| {
            table.len() == 1 && table.get("path").and_then(Value::as_str) == Some(expected)
        })
}

fn is_exact_workspace(specification: Option<&Value>) -> bool {
    specification
        .and_then(Value::as_table)
        .is_some_and(|table| {
            table.len() == 1 && table.get("workspace").and_then(Value::as_bool) == Some(true)
        })
}
