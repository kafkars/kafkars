//! Exact ownership, capability, and method guards for the transaction facade.

mod support;

use support::{
    CapabilityRule, LinearOwner, MethodCapabilityRule, WalkScope, capability_violations,
    fixture_files, linear_violations, load_config, method_capability_violations, read,
    rust_files_under, workspace_root,
};
use syn::{ImplItem, ItemImpl, Type, Visibility, visit::Visit};

const HANDLE: &str = "crates/kafka-client/src/bridge/transaction/handle.rs";
const LINEAR: &[(&str, &str)] = &[
    ("TransactionalProducerInitializer", HANDLE),
    (
        "TransactionInitialization",
        "crates/kafka-client/src/bridge/transaction/operation.rs",
    ),
    (
        "TransactionalProducerEngine",
        "crates/kafka-client/src/bridge/transaction/owner.rs",
    ),
    (
        "TransactionalProducerBuilder",
        "crates/kafka-client/src/transaction/builder.rs",
    ),
    (
        "InitializeTransactionalProducer",
        "crates/kafka-client/src/transaction/initialization.rs",
    ),
    (
        "TransactionalProducer",
        "crates/kafka-client/src/transaction/producer.rs",
    ),
];
const METHODS: &[&str] = &[
    "capture_transactional_owner_initialization",
    "initialize_transactional_owner",
];
const LIFECYCLE_METHODS: &[&str] = &["begin", "send", "send_offsets", "commit", "abort"];
const PUBLIC_LIFECYCLE: &[(&str, &str)] = &[
    ("TransactionalProducer", "begin"),
    ("Transaction", "commit"),
    ("Transaction", "abort"),
];

#[test]
fn checked_in_transaction_facade_ownership_is_exact() {
    let config = load_config(&workspace_root());
    for (owner_type, path) in LINEAR {
        let rules = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(rules[0].path, *path);
    }
    for method in METHODS {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == *method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method owner");
        assert_eq!(rules[0].allowed_paths, [HANDLE]);
    }
    let capability = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == "crates/kafka-client/src/transaction")
        .collect::<Vec<_>>();
    assert_eq!(capability.len(), 1);
    assert_eq!(
        capability[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["kafka_client_engine"]
    );
}

#[test]
fn fixture_rejects_clone_and_copy_for_every_facade_owner() {
    let (root, files) = fixture_files("transaction_initialization_facade");
    let rules = LINEAR
        .iter()
        .map(|(owner_type, _path)| LinearOwner {
            owner_type: (*owner_type).to_owned(),
            path: "src/linear_intruder.rs".to_owned(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &rules);
    for (owner_type, _path) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }
}

#[test]
fn fixture_rejects_public_engine_import_and_second_submission_owner() {
    let (root, _files) = fixture_files("transaction_initialization_facade");
    let capability = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".to_owned(),
            forbidden: vec!["kafka_client_engine".to_owned()],
            allow: Vec::new(),
        }],
    );
    assert!(
        capability
            .iter()
            .any(|violation| violation.contains("kafka_client_engine"))
    );

    let rules = METHODS
        .iter()
        .map(|method| MethodCapabilityRule {
            root: "src".to_owned(),
            method: (*method).to_owned(),
            allowed_paths: vec!["src/submission_owner.rs".to_owned()],
        })
        .collect::<Vec<_>>();
    let violations = method_capability_violations(&root, &rules);
    for method in METHODS {
        assert!(violations.iter().any(|violation| {
            violation.contains("submission_intruder.rs") && violation.contains(method)
        }));
    }
    assert!(
        !violations
            .iter()
            .any(|violation| violation.contains("submission_owner.rs"))
    );
}

#[test]
fn public_transaction_lifecycle_has_exact_supported_owners() {
    let workspace = workspace_root();
    let live = rust_files_under(
        &workspace.join("crates/kafka-client/src"),
        WalkScope::Fixture,
    );
    let mut actual = public_lifecycle_methods(&live);
    actual.sort();
    let mut expected = PUBLIC_LIFECYCLE
        .iter()
        .map(|(owner, method)| ((*owner).to_owned(), (*method).to_owned()))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(actual, expected, "public transaction lifecycle drifted");

    let (fixture, _) = fixture_files("transaction_initialization_facade");
    let violations = public_lifecycle_methods(&[fixture.join("src/lifecycle_intruder.rs")]);
    for method in LIFECYCLE_METHODS {
        assert!(
            violations
                .iter()
                .any(|(owner, violation)| owner == "TransactionalProducer" && violation == method),
            "negative fixture did not expose public {method}: {violations:?}"
        );
    }

    let outside = public_lifecycle_methods(&[fixture.join("src/bridge/lifecycle_intruder.rs")]);
    for method in LIFECYCLE_METHODS {
        assert!(
            outside
                .iter()
                .filter(|(owner, violation)| {
                    owner == "TransactionalProducer" && violation == method
                })
                .count()
                >= 2,
            "outside inherent impl and public trait did not expose {method}: {outside:?}"
        );
    }
}

fn public_lifecycle_methods(files: &[std::path::PathBuf]) -> Vec<(String, String)> {
    let mut methods = Vec::new();
    for file in files {
        let syntax = syn::parse_file(&read(file))
            .unwrap_or_else(|error| panic!("parse {}: {error}", file.display()));
        let mut collector = PublicLifecycleCollector {
            methods: &mut methods,
        };
        collector.visit_file(&syntax);
    }
    methods
}

struct PublicLifecycleCollector<'a> {
    methods: &'a mut Vec<(String, String)>,
}

impl<'ast> Visit<'ast> for PublicLifecycleCollector<'_> {
    fn visit_item_impl(&mut self, implementation: &'ast ItemImpl) {
        if let Some(owner) = public_transaction_owner(&implementation.self_ty) {
            for item in &implementation.items {
                let ImplItem::Fn(function) = item else {
                    continue;
                };
                let public_inherent = implementation.trait_.is_none()
                    && matches!(function.vis, Visibility::Public(_));
                let trait_method = implementation.trait_.is_some();
                if (public_inherent || trait_method)
                    && LIFECYCLE_METHODS.contains(&function.sig.ident.to_string().as_str())
                {
                    self.methods
                        .push((owner.to_owned(), function.sig.ident.to_string()));
                }
            }
        }
        syn::visit::visit_item_impl(self, implementation);
    }
}

fn public_transaction_owner(target: &Type) -> Option<&'static str> {
    match target {
        Type::Group(group) => public_transaction_owner(&group.elem),
        Type::Paren(paren) => public_transaction_owner(&paren.elem),
        Type::Path(path) => match path.path.segments.last()?.ident.to_string().as_str() {
            "TransactionalProducer" => Some("TransactionalProducer"),
            "Transaction" => Some("Transaction"),
            _ => None,
        },
        Type::Reference(reference) => public_transaction_owner(&reference.elem),
        _ => None,
    }
}
