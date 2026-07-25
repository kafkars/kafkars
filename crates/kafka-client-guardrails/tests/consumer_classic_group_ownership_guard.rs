//! Exact structural ratchets for deterministic classic Join and Sync policy.

#[path = "consumer_classic_group_ownership_guard/expectations.rs"]
mod expectations;
mod support;

use support::{
    CapabilityRule, LinearOwner, MutationOwner, capability_violations, fixture_files,
    linear_violations, load_config, mutation_violations, workspace_root,
};

use expectations::{FORBIDDEN, LINEAR, MACHINE, MACHINE_FIELDS, MIRRORS, ROOT};

#[test]
fn checked_in_classic_group_mirrors_and_linear_owners_are_exact() {
    let config = load_config(&workspace_root());
    let actual_mirrors = config
        .test_mirrors
        .iter()
        .filter(|rule| rule.production.starts_with(&format!("{ROOT}/")))
        .map(|rule| (rule.production.clone(), rule.test.clone()))
        .collect::<Vec<_>>();
    let expected_mirrors = MIRRORS
        .iter()
        .map(|(production, test)| (format!("{ROOT}/{production}"), format!("{ROOT}/{test}")))
        .collect::<Vec<_>>();
    assert_eq!(actual_mirrors, expected_mirrors);

    let actual_linear = config
        .linear_owners
        .iter()
        .filter(|rule| rule.path.starts_with(&format!("{ROOT}/")))
        .map(|rule| (rule.owner_type.clone(), rule.path.clone()))
        .collect::<Vec<_>>();
    let expected_linear = LINEAR
        .iter()
        .map(|(owner_type, file)| ((*owner_type).into(), format!("{ROOT}/{file}")))
        .collect::<Vec<(String, String)>>();
    assert_eq!(actual_linear, expected_linear);
    for (owner_type, file) in LINEAR {
        let matches = config
            .linear_owners
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(matches.len(), 1, "{owner_type} needs one linear rule");
        assert_eq!(matches[0].path, format!("{ROOT}/{file}"));
    }
}

#[test]
fn checked_in_classic_group_mutation_and_capability_policy_is_exact() {
    let config = load_config(&workspace_root());
    let actual_fields = config
        .mutation_owners
        .iter()
        .filter(|rule| rule.owner_type == "ClassicGroupMachine")
        .map(|rule| (rule.field.clone(), rule.allowed_paths.clone()))
        .collect::<Vec<_>>();
    let expected_fields = MACHINE_FIELDS
        .iter()
        .map(|(field, paths)| {
            (
                (*field).to_owned(),
                paths
                    .iter()
                    .map(|path| (*path).to_owned())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(actual_fields, expected_fields);
    assert_eq!(
        declared_machine_fields(),
        std::iter::once("group_id")
            .chain(MACHINE_FIELDS.iter().map(|(field, _paths)| *field))
            .map(str::to_owned)
            .collect::<Vec<_>>()
    );

    let capabilities = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == ROOT)
        .collect::<Vec<_>>();
    assert_eq!(capabilities.len(), 1);
    assert_eq!(
        capabilities[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        FORBIDDEN
    );
    assert!(capabilities[0].allow.is_empty());
}

#[test]
fn fixture_rejects_each_clone_copy_and_foreign_machine_mutation() {
    let (root, files) = fixture_files("consumer_classic_group_ownership");
    let linear_rules = LINEAR
        .iter()
        .map(|(owner_type, _file)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let linear = linear_violations(&root, &files, &linear_rules);
    for (owner_type, _file) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(linear.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }

    let mutation_rules = MACHINE_FIELDS
        .iter()
        .map(|(field, _paths)| MutationOwner {
            owner_type: "ClassicGroupMachine".into(),
            field: (*field).into(),
            allowed_paths: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mutations = mutation_violations(&root, &files, &mutation_rules);
    for (field, _paths) in MACHINE_FIELDS {
        assert!(mutations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains("ClassicGroupMachine")
                && violation.contains(field)
        }));
    }
}

#[test]
fn fixture_rejects_every_forbidden_classic_group_capability() {
    let (root, _files) = fixture_files("consumer_classic_group_ownership");
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src".into(),
            forbidden: FORBIDDEN.iter().map(|value| (*value).into()).collect(),
            allow: Vec::new(),
        }],
    );
    for capability in FORBIDDEN {
        assert!(
            violations.iter().any(|violation| {
                violation.contains("capability_intruder.rs") && violation.contains(capability)
            }),
            "capability detector missed {capability}: {violations:?}"
        );
    }
}

fn declared_machine_fields() -> Vec<String> {
    let source = std::fs::read_to_string(workspace_root().join(MACHINE))
        .unwrap_or_else(|error| panic!("read classic group machine: {error}"));
    let syntax = syn::parse_file(&source)
        .unwrap_or_else(|error| panic!("parse classic group machine: {error}"));
    syntax
        .items
        .iter()
        .find_map(|item| {
            let syn::Item::Struct(owner) = item else {
                return None;
            };
            (owner.ident == "ClassicGroupMachine").then(|| {
                owner
                    .fields
                    .iter()
                    .map(|field| {
                        field
                            .ident
                            .as_ref()
                            .unwrap_or_else(|| panic!("machine field must be named"))
                            .to_string()
                    })
                    .collect()
            })
        })
        .unwrap_or_else(|| panic!("ClassicGroupMachine declaration missing"))
}
