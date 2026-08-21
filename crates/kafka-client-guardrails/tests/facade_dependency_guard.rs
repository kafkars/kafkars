//! Facade source reaches execution only through its private engine bridge.

mod support;

use std::collections::BTreeSet;
use std::path::Path;

use support::{WalkScope, display_path, fixture_files, rust_files_under, workspace_root};
use syn::visit::Visit;
use syn::{ItemExternCrate, ItemUse, UseTree};

const ENGINE: &str = "kafka_client_engine";
const FORBIDDEN: [&str; 4] = [
    "kafka_client_core",
    "kafka_driver",
    "kafka_wire",
    "kafka_wire_records",
];

fn facade_dependency_violations(source_root: &Path) -> Vec<String> {
    let mut violations = Vec::new();
    for path in rust_files_under(source_root, WalkScope::Fixture) {
        let relative = display_path(source_root, &path);
        let source = support::read(&path);
        let syntax =
            syn::parse_file(&source).unwrap_or_else(|error| panic!("parse {relative}: {error}"));
        let mut collector = DependencyCollector::default();
        collector.visit_file(&syntax);

        for dependency in collector.dependencies {
            if dependency == ENGINE && !is_bridge_file(source_root, &path) {
                violations.push(format!(
                    "{relative} imports {ENGINE} outside the private bridge"
                ));
            } else if FORBIDDEN.contains(&dependency.as_str()) {
                violations.push(format!("{relative} imports forbidden crate {dependency}"));
            }
        }
    }
    violations
}

fn is_bridge_file(source_root: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(source_root) else {
        return false;
    };
    relative == Path::new("bridge.rs")
        || relative
            .components()
            .next()
            .is_some_and(|component| component.as_os_str() == "bridge")
}

#[derive(Default)]
struct DependencyCollector {
    dependencies: BTreeSet<String>,
}

impl DependencyCollector {
    fn observe(&mut self, value: &str) {
        if value == ENGINE || FORBIDDEN.contains(&value) {
            self.dependencies.insert(value.to_owned());
        }
    }
}

impl<'ast> Visit<'ast> for DependencyCollector {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        if let Some(segment) = path.segments.first() {
            self.observe(&segment.ident.to_string());
        }
        syn::visit::visit_path(self, path);
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        collect_use_roots(&item.tree, self);
        syn::visit::visit_item_use(self, item);
    }

    fn visit_item_extern_crate(&mut self, item: &'ast ItemExternCrate) {
        self.observe(&item.ident.to_string());
        syn::visit::visit_item_extern_crate(self, item);
    }
}

fn collect_use_roots(tree: &UseTree, collector: &mut DependencyCollector) {
    match tree {
        UseTree::Path(path) => collector.observe(&path.ident.to_string()),
        UseTree::Name(name) => collector.observe(&name.ident.to_string()),
        UseTree::Rename(rename) => collector.observe(&rename.ident.to_string()),
        UseTree::Group(group) => {
            for item in &group.items {
                collect_use_roots(item, collector);
            }
        }
        UseTree::Glob(_) => {}
    }
}

#[test]
fn live_facade_dependencies_respect_the_private_bridge() {
    let source_root = workspace_root().join("crates/kafkars/src");
    let violations = facade_dependency_violations(&source_root);

    assert!(
        violations.is_empty(),
        "facade dependency boundary violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn engine_bypasses_and_lower_layer_imports_are_rejected() {
    let (root, _) = fixture_files("facade_dependency_boundary");
    let violations = facade_dependency_violations(&root.join("src"));

    assert!(violations.iter().any(|value| {
        value.contains("public_api.rs") && value.contains("outside the private bridge")
    }));
    for forbidden in FORBIDDEN {
        assert!(
            violations.iter().any(|value| value.contains(forbidden)),
            "facade detector accepted {forbidden}: {violations:?}"
        );
    }
    assert!(
        !violations.iter().any(|value| value.contains("allowed.rs")),
        "facade detector rejected the sanctioned bridge: {violations:?}"
    );
}
