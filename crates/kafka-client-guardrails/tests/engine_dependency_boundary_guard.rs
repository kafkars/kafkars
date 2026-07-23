//! Driver and wire dependencies remain confined to their engine-owned adapters.

mod support;

use std::collections::BTreeSet;
use std::path::{Component, Path};

use support::{WalkScope, display_path, fixture_files, rust_files_under, workspace_root};
use syn::visit::Visit;
use syn::{ItemExternCrate, ItemUse, UseTree};

const DRIVER: &str = "kafka_driver";
const WIRE: [&str; 2] = ["kafka_wire", "kafka_wire_records"];

fn boundary_violations(source_root: &Path) -> Vec<String> {
    let mut violations = Vec::new();
    for path in rust_files_under(source_root, WalkScope::Fixture) {
        let relative = path.strip_prefix(source_root).unwrap_or(&path);
        let source = support::read(&path);
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", display_path(source_root, &path)));
        let mut collector = DependencyCollector::default();
        collector.visit_file(&syntax);

        for dependency in collector.dependencies {
            let allowed = if dependency == DRIVER {
                is_beneath(relative, "driver")
            } else {
                is_beneath(relative, "protocol")
            };
            if !allowed {
                violations.push(format!(
                    "{} imports {dependency} outside its engine adapter",
                    display_path(source_root, &path)
                ));
            }
        }
    }
    violations
}

fn is_beneath(path: &Path, directory: &str) -> bool {
    matches!(
        path.components().next(),
        Some(Component::Normal(component)) if component == directory
    )
}

#[derive(Default)]
struct DependencyCollector {
    dependencies: BTreeSet<String>,
}

impl DependencyCollector {
    fn observe(&mut self, value: &str) {
        if value == DRIVER || WIRE.contains(&value) {
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
fn live_engine_dependencies_stay_in_their_adapters() {
    let source_root = workspace_root().join("crates/kafka-client-engine/src");
    let violations = boundary_violations(&source_root);

    assert!(
        violations.is_empty(),
        "engine dependency boundary violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn aliases_and_qualified_paths_cannot_bypass_engine_adapters() {
    let (root, _) = fixture_files("engine_dependency_boundary");
    let violations = boundary_violations(&root.join("src"));

    for file in [
        "driver_alias.rs",
        "driver_forbidden.rs",
        "wire_qualified.rs",
        "records_alias.rs",
    ] {
        assert!(
            violations.iter().any(|value| value.contains(file)),
            "engine boundary detector accepted {file}: {violations:?}"
        );
    }
    assert!(
        !violations.iter().any(|value| value.contains("allowed.rs")),
        "engine boundary detector rejected sanctioned adapters: {violations:?}"
    );
}
