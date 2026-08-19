//! Ownership registration and negative evidence for the Fetch delivery store.

mod support;

use support::{
    CallCapabilityRule, LinearOwner, MethodCapabilityRule, MutationOwner,
    call_capability_violations, fixture_files, linear_violations, load_config,
    method_capability_violations, mutation_violations, workspace_root,
};

const STORE: &str = "crates/kafka-client-engine/src/consumer/fetch_store.rs";
const BATCH: &str = "crates/kafka-client-engine/src/consumer/fetch_store/batch.rs";
const DELIVERY: &str = "crates/kafka-client-engine/src/consumer/fetch_store/delivery.rs";
const RETENTION: &str = "crates/kafka-client-engine/src/protocol/fetch/retention.rs";
const LINEAR: &[(&str, &str)] = &[
    ("FetchReservationDomain", RETENTION),
    ("FetchStoreReservation", STORE),
    ("FetchStageProof", STORE),
    ("FetchSlot", STORE),
    ("FetchDeliveryStore", STORE),
    ("FetchDelivery", DELIVERY),
];
const MUTATIONS: &[(&str, &str, &[&str])] = &[
    ("FetchDeliveryStore", "next_sequence", &[STORE, BATCH]),
    ("FetchDeliveryStore", "next_authorization", &[DELIVERY]),
    ("FetchDeliveryStore", "used_bytes", &[STORE, BATCH]),
    ("FetchDeliveryStore", "slots", &[STORE, DELIVERY, BATCH]),
    ("FetchSlot", "charged_bytes", &[STORE]),
    ("FetchSlot", "provenance", &[STORE]),
    ("FetchSlot", "outcome", &[STORE]),
    ("FetchSlot", "state", &[STORE]),
];

#[test]
fn checked_in_store_owners_are_exact_and_linear() {
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
    for (owner_type, field, allowed_paths) in MUTATIONS {
        let rules = config
            .mutation_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type && rule.field == *field)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type}.{field} needs one owner");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *allowed_paths,
        );
    }
}

#[test]
fn reservation_domain_minting_stays_with_the_store() {
    let config = load_config(&workspace_root());
    let constructors = config
        .call_capabilities
        .iter()
        .filter(|rule| rule.call == "FetchReservationDomain::create_store_domain")
        .collect::<Vec<_>>();
    assert_eq!(constructors.len(), 1);
    assert_eq!(constructors[0].allowed_paths, [STORE]);
    let issuers = config
        .method_capabilities
        .iter()
        .filter(|rule| rule.method == "issue_pair")
        .collect::<Vec<_>>();
    assert_eq!(issuers.len(), 1);
    assert_eq!(issuers[0].allowed_paths, [STORE, BATCH]);

    let root = fixture_files("consumer_fetch_ownership").0;
    let call_violations = call_capability_violations(
        &root,
        &[CallCapabilityRule {
            root: "src".into(),
            call: "FetchReservationDomain::create_store_domain".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(call_violations.iter().any(|violation| {
        violation.contains("reservation_domain_intruder.rs")
            && violation.contains("FetchReservationDomain::create_store_domain")
    }));
    assert!(call_violations.iter().any(|violation| {
        violation.contains("reservation_domain_self_intruder.rs")
            && violation.contains("FetchReservationDomain::create_store_domain")
    }));
    assert!(
        !call_violations
            .iter()
            .any(|violation| violation.contains("reservation_intruder.rs")),
        "an unrelated `new` constructor was treated as the protected domain: {call_violations:?}"
    );
    let method_violations = method_capability_violations(
        &root,
        &[MethodCapabilityRule {
            root: "src".into(),
            method: "issue_pair".into(),
            allowed_paths: Vec::new(),
        }],
    );
    assert!(method_violations.iter().any(|violation| {
        violation.contains("reservation_domain_intruder.rs") && violation.contains("issue_pair")
    }));
}

#[test]
fn fixture_rejects_store_duplication_and_foreign_mutation() {
    let (root, files) = fixture_files("consumer_fetch_ownership");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let linear_violations = linear_violations(&root, &files, &linear);
    for (owner_type, _) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(linear_violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }

    let mutations = MUTATIONS
        .iter()
        .map(|(owner_type, field, _)| MutationOwner {
            owner_type: (*owner_type).into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mutation_violations = mutation_violations(&root, &files, &mutations);
    for (owner_type, field, _) in MUTATIONS {
        assert!(mutation_violations.iter().any(|violation| {
            violation.contains("store_mutation_intruder.rs")
                && violation.contains(owner_type)
                && violation.contains(field)
        }));
    }
}
