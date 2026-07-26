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
const UNLANDED_LIFECYCLE: &[&str] = &["begin", "send", "commit", "abort"];

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
fn public_transaction_lifecycle_waits_for_real_policy_owners() {
    let workspace = workspace_root();
    let live = rust_files_under(
        &workspace.join("crates/kafka-client/src"),
        WalkScope::Fixture,
    );
    assert!(
        public_lifecycle_methods(&live).is_empty(),
        "unfinished transaction lifecycle entered the public facade"
    );

    let (fixture, _) = fixture_files("transaction_initialization_facade");
    let violations = public_lifecycle_methods(&[fixture.join("src/lifecycle_intruder.rs")]);
    for method in UNLANDED_LIFECYCLE {
        assert!(
            violations.iter().any(|violation| violation == method),
            "negative fixture did not expose public {method}: {violations:?}"
        );
    }

    let outside = public_lifecycle_methods(&[fixture.join("src/bridge/lifecycle_intruder.rs")]);
    for method in UNLANDED_LIFECYCLE {
        assert!(
            outside
                .iter()
                .filter(|violation| violation.as_str() == *method)
                .count()
                >= 2,
            "outside inherent impl and public trait did not expose {method}: {outside:?}"
        );
    }
}

fn public_lifecycle_methods(files: &[std::path::PathBuf]) -> Vec<String> {
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
    methods: &'a mut Vec<String>,
}

impl<'ast> Visit<'ast> for PublicLifecycleCollector<'_> {
    fn visit_item_impl(&mut self, implementation: &'ast ItemImpl) {
        if is_transactional_producer(&implementation.self_ty) {
            for item in &implementation.items {
                let ImplItem::Fn(function) = item else {
                    continue;
                };
                let public_inherent = implementation.trait_.is_none()
                    && matches!(function.vis, Visibility::Public(_));
                let trait_method = implementation.trait_.is_some();
                if (public_inherent || trait_method)
                    && UNLANDED_LIFECYCLE.contains(&function.sig.ident.to_string().as_str())
                {
                    self.methods.push(function.sig.ident.to_string());
                }
            }
        }
        syn::visit::visit_item_impl(self, implementation);
    }
}

fn is_transactional_producer(target: &Type) -> bool {
    match target {
        Type::Group(group) => is_transactional_producer(&group.elem),
        Type::Paren(paren) => is_transactional_producer(&paren.elem),
        Type::Path(path) => path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "TransactionalProducer"),
        Type::Reference(reference) => is_transactional_producer(&reference.elem),
        _ => false,
    }
}
