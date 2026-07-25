//! Ownership, mirror, and capability ratchets for follower membership composition.

#[path = "consumer_classic_group_follower_ownership_guard/expectations.rs"]
mod expectations;
mod support;

use std::collections::BTreeSet;

use support::{
    AuthorityToken, CapabilityRule, LinearOwner, authority_token_violations, capability_violations,
    fixture_files, linear_violations, load_config, workspace_root,
};

use expectations::{
    AUTHORITIES, BASE_FORBIDDEN, CAPABILITIES, CAPABILITY_ALLOWS, ENTRY_FAULT,
    ENTRY_FAULT_VARIANTS, GROUP_ROOT, LINEAR, MIRRORS,
};

#[test]
fn checked_in_follower_owners_and_mirrors_are_exact() {
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
    for (owner_type, path, fields) in AUTHORITIES {
        let rules = config
            .authority_tokens
            .iter()
            .filter(|rule| rule.owner_type == *owner_type)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{owner_type} needs one authority rule");
        assert_eq!(rules[0].path, *path);
        assert_eq!(
            rules[0]
                .fields
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *fields
        );
        assert_eq!(rules[0].allowed_paths, [*path]);
    }
    for (production, test) in MIRRORS {
        let production = format!("{GROUP_ROOT}/{production}");
        let rules = config
            .test_mirrors
            .iter()
            .filter(|rule| rule.production == production)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{production} needs one test mirror");
        assert_eq!(rules[0].test, format!("{GROUP_ROOT}/{test}"));
    }
}

#[test]
fn checked_in_follower_capability_sets_are_exact() {
    let config = load_config(&workspace_root());
    for (path, extras) in CAPABILITIES {
        let rules = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == *path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{path} needs one capability rule");
        let actual = rules[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let expected = BASE_FORBIDDEN
            .iter()
            .copied()
            .chain(extras.iter().copied())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected, "{path} capability set");
        assert!(rules[0].allow.is_empty());
    }
    for (path, capability) in CAPABILITY_ALLOWS {
        let rules = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == *path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{path} needs one capability rule");
        assert_eq!(rules[0].allow.len(), 1);
        assert_eq!(rules[0].allow[0].path, *path);
        assert_eq!(rules[0].allow[0].capability, *capability);
        assert!(!rules[0].allow[0].reason.trim().is_empty());
    }
}

#[test]
fn entry_fault_variants_are_closed_and_exact() {
    let source = std::fs::read_to_string(workspace_root().join(ENTRY_FAULT))
        .unwrap_or_else(|error| panic!("read {ENTRY_FAULT}: {error}"));
    let syntax =
        syn::parse_file(&source).unwrap_or_else(|error| panic!("parse {ENTRY_FAULT}: {error}"));
    let actual = syntax
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Enum(item) if item.ident == "ClassicGroupEntryFault" => Some(
                item.variants
                    .iter()
                    .map(|variant| variant.ident.to_string())
                    .collect::<BTreeSet<_>>(),
            ),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{ENTRY_FAULT} does not declare ClassicGroupEntryFault"));
    assert_eq!(
        actual,
        ENTRY_FAULT_VARIANTS
            .iter()
            .map(|variant| (*variant).to_owned())
            .collect()
    );
}

#[test]
fn fixture_rejects_cloneable_and_foreignly_constructed_owners() {
    let (root, files) = fixture_files("consumer_classic_group_follower_ownership");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _path)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for (owner_type, _path) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
                violation.contains(owner_type) && violation.contains(derived)
            }));
        }
    }
    let authorities = AUTHORITIES
        .iter()
        .map(|(owner_type, _path, fields)| AuthorityToken {
            owner_type: (*owner_type).into(),
            path: "src/authority_owner.rs".into(),
            fields: fields.iter().map(|field| (*field).into()).collect(),
            allowed_paths: vec!["src/authority_owner.rs".into()],
        })
        .collect::<Vec<_>>();
    let violations = authority_token_violations(&root, &files, &authorities);
    for (owner_type, _path, _fields) in AUTHORITIES {
        assert!(violations.iter().any(|violation| {
            violation.contains("authority_intruder.rs") && violation.contains(owner_type)
        }));
    }
}

#[test]
fn fixture_rejects_every_follower_capability() {
    let (root, _files) = fixture_files("consumer_classic_group_follower_ownership");
    let forbidden = BASE_FORBIDDEN
        .iter()
        .copied()
        .chain(["crate::clock", "crate::driver", "crate::protocol"])
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let violations = capability_violations(
        &root,
        &[CapabilityRule {
            root: "src/capability_intruder.rs".into(),
            forbidden: forbidden.clone(),
            allow: Vec::new(),
        }],
    );
    for capability in forbidden {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(&capability)),
            "capability detector missed {capability}: {violations:?}"
        );
    }
}
