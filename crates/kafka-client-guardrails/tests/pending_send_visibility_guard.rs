//! Waiting producer admission remains crate-private until lifecycle settlement exists.

mod support;

use syn::{ImplItem, Item, Type, Visibility};

use support::{fixture_files, read, workspace_root};

const HANDLE: &str = "crates/kafka-client-engine/src/producer/boundary/handle.rs";
const PROTECTED_METHODS: [&str; 2] = ["send", "send_captured"];

#[test]
fn live_waiting_send_methods_are_crate_private() {
    let workspace = workspace_root();
    let violations = visibility_violations(&read(&workspace.join(HANDLE)));

    assert!(
        violations.is_empty(),
        "waiting send became reachable before lifecycle settlement:\n{}",
        violations.join("\n")
    );
}

#[test]
fn public_visibility_fixture_is_rejected_and_crate_visibility_is_accepted() {
    let (_root, files) = fixture_files("pending_send_visibility");
    let mut public = None;
    let mut restricted = None;
    for path in files {
        let violations = visibility_violations(&read(&path));
        match path.file_name().and_then(|name| name.to_str()) {
            Some("public.rs") => public = Some(violations),
            Some("restricted.rs") => restricted = Some(violations),
            _ => {}
        }
    }

    assert!(
        public.is_some_and(|violations| !violations.is_empty()),
        "public waiting send fixture escaped the visibility ratchet"
    );
    assert_eq!(
        restricted.unwrap_or_else(|| panic!("restricted fixture is missing")),
        Vec::<String>::new()
    );
}

fn visibility_violations(source: &str) -> Vec<String> {
    let syntax =
        syn::parse_file(source).unwrap_or_else(|error| panic!("parse visibility source: {error}"));
    let mut observed = Vec::new();
    let mut violations = Vec::new();
    for item in syntax.items {
        let Item::Impl(implementation) = item else {
            continue;
        };
        let Type::Path(owner) = implementation.self_ty.as_ref() else {
            continue;
        };
        match owner.path.segments.last() {
            Some(segment) if segment.ident == "ProducerHandle" => {}
            _ => continue,
        }
        for item in implementation.items {
            let ImplItem::Fn(method) = item else {
                continue;
            };
            let name = method.sig.ident.to_string();
            if !PROTECTED_METHODS.contains(&name.as_str()) {
                continue;
            }
            observed.push(name.clone());
            if !crate_visibility(&method.vis) {
                violations.push(format!("ProducerHandle::{name} must remain pub(crate)"));
            }
        }
    }
    for expected in PROTECTED_METHODS {
        if !observed.iter().any(|name| name == expected) {
            violations.push(format!(
                "protected ProducerHandle::{expected} method is missing"
            ));
        }
    }
    violations
}

fn crate_visibility(visibility: &Visibility) -> bool {
    let Visibility::Restricted(restricted) = visibility else {
        return false;
    };
    restricted.path.is_ident("crate")
}
