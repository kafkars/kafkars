//! Exact Cargo manifest and lock contracts for published Kafka dependencies.

use toml::Value;

const DEPENDENCY_SECTIONS: [&str; 3] = ["dependencies", "dev-dependencies", "build-dependencies"];
const CRATES_IO: &str = "registry+https://github.com/rust-lang/crates.io-index";

#[derive(Clone, Copy)]
struct PublishedDependency {
    name: &'static str,
    version: &'static str,
}

const PUBLISHED_DEPENDENCIES: [PublishedDependency; 4] = [
    PublishedDependency {
        name: "kafka-driver",
        version: "=0.1.0-rc.8",
    },
    PublishedDependency {
        name: "kafka-wire",
        version: "=0.1.0",
    },
    PublishedDependency {
        name: "kafka-wire-records",
        version: "=0.1.0",
    },
    PublishedDependency {
        name: "kafka-wire-core",
        version: "=0.1.0",
    },
];

const PUBLISHED: [(&str, &str, &str); 6] = [
    (
        "kafka-driver",
        "0.1.0-rc.8",
        "4785bae496fdaeb39e52e4a84a2e588525a235ee63a967fd08a6aaa3c35bfaba",
    ),
    (
        "kafka-driver-core",
        "0.1.0-rc.8",
        "fa4ee71408e6b01c261002e84b26ef8e1a54bd38f28b5ab63438c69a20d36410",
    ),
    (
        "kafka-driver-transport",
        "0.1.0-rc.8",
        "62b5dd98013d34ef2d8b5ec3bd0f3ac29d95882b9fd2ae0cc4251531e3aefd19",
    ),
    (
        "kafka-wire",
        "0.1.0",
        "4a6bbde74286d49bf2f8531e64bb46132824a03bf6058583c0c79dd5ebd00e78",
    ),
    (
        "kafka-wire-core",
        "0.1.0",
        "85717dc7f7e3530cc197a92b5087bd7aa975904ed657edbc6ad5c4184e045203",
    ),
    (
        "kafka-wire-records",
        "0.1.0",
        "aafe0f476f8e7ca72401bced93523612b060dc3f5f0039a8ee82177c44e39add",
    ),
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

pub(crate) fn lock_violations(source: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let Some(lock) = parse("lockfile", source, &mut violations) else {
        return violations;
    };
    let Some(packages) = lock.get("package").and_then(Value::as_array) else {
        violations.push("lockfile must contain a package array".to_owned());
        return violations;
    };
    for (name, version, checksum) in PUBLISHED {
        let matching = packages
            .iter()
            .filter(|package| package.get("name").and_then(Value::as_str) == Some(name))
            .collect::<Vec<_>>();
        let exact = matching.len() == 1
            && matching[0].get("version").and_then(Value::as_str) == Some(version)
            && matching[0].get("source").and_then(Value::as_str) == Some(CRATES_IO)
            && matching[0].get("checksum").and_then(Value::as_str) == Some(checksum);
        if !exact {
            violations.push(format!(
                "lockfile must bind {name} {version} to its exact crates.io checksum"
            ));
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

fn inspect_root(root: &Value, violations: &mut Vec<String>) {
    reject_overrides("workspace", root, violations);
    let workspace_dependencies = root
        .get("workspace")
        .and_then(|workspace| workspace.get("dependencies"))
        .and_then(Value::as_table);

    for dependency in PUBLISHED_DEPENDENCIES {
        let specification =
            workspace_dependencies.and_then(|dependencies| dependencies.get(dependency.name));
        if !is_exact_registry_version(specification, dependency.version) {
            violations.push(format!(
                "workspace dependency {} must be exact registry requirement \"{}\"",
                dependency.name, dependency.version
            ));
        }
    }
    if let Some(dependencies) = workspace_dependencies {
        reject_aliases("workspace dependencies", dependencies, violations);
    }
    reject_top_level_published("workspace root", root, violations);
    reject_target_published("workspace root", root, violations);
}

fn inspect_engine(engine: &Value, violations: &mut Vec<String>) {
    reject_overrides("engine", engine, violations);
    let dependencies = engine.get("dependencies").and_then(Value::as_table);
    for dependency in PUBLISHED_DEPENDENCIES {
        let specification = dependencies.and_then(|values| values.get(dependency.name));
        if !is_exact_workspace(specification) {
            violations.push(format!(
                "engine dependency {} must be exactly {{ workspace = true }}",
                dependency.name
            ));
        }
    }
    if let Some(dependencies) = dependencies {
        reject_aliases("engine dependencies", dependencies, violations);
    }
    if let Some(dependencies) = engine.get("dev-dependencies").and_then(Value::as_table) {
        reject_published("engine dev-dependencies", dependencies, violations);
    }
    if let Some(dependencies) = engine.get("build-dependencies").and_then(Value::as_table) {
        reject_published("engine build-dependencies", dependencies, violations);
    }
    reject_target_published("engine", engine, violations);
}

fn reject_overrides(label: &str, manifest: &Value, violations: &mut Vec<String>) {
    for section in ["patch", "replace"] {
        if manifest.get(section).is_some() {
            violations.push(format!("{label} manifest may not declare [{section}]"));
        }
    }
}

fn reject_top_level_published(label: &str, manifest: &Value, violations: &mut Vec<String>) {
    for section in DEPENDENCY_SECTIONS {
        if let Some(dependencies) = manifest.get(section).and_then(Value::as_table) {
            reject_published(&format!("{label} {section}"), dependencies, violations);
        }
    }
}

fn reject_target_published(label: &str, manifest: &Value, violations: &mut Vec<String>) {
    let Some(targets) = manifest.get("target").and_then(Value::as_table) else {
        return;
    };
    for (selector, target) in targets {
        for section in DEPENDENCY_SECTIONS {
            if let Some(dependencies) = target.get(section).and_then(Value::as_table) {
                reject_published(
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
        if is_published(package) && declared != package {
            violations.push(format!(
                "{label} aliases reviewed package {package} as {declared}"
            ));
        }
    }
}

fn reject_published(
    label: &str,
    dependencies: &toml::map::Map<String, Value>,
    violations: &mut Vec<String>,
) {
    for (declared, specification) in dependencies {
        let package = package_name(declared, specification);
        if is_published(package) {
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

fn is_published(name: &str) -> bool {
    PUBLISHED_DEPENDENCIES
        .iter()
        .any(|dependency| dependency.name == name)
}

fn is_exact_registry_version(specification: Option<&Value>, expected_version: &str) -> bool {
    specification.and_then(Value::as_str) == Some(expected_version)
}

fn is_exact_workspace(specification: Option<&Value>) -> bool {
    specification
        .and_then(Value::as_table)
        .is_some_and(|table| {
            table.len() == 1 && table.get("workspace").and_then(Value::as_bool) == Some(true)
        })
}
