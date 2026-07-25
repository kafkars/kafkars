//! Exact capability ratchet for hosted private group-consumer scheduling.

#[path = "engine_host_group_consumer_guard/expectations.rs"]
mod expectations;
mod support;

use support::{CapabilityRule, capability_violations, fixture_files, load_config, workspace_root};

use expectations::{ROOT, RULES};

#[test]
fn checked_in_group_host_rules_are_exact() {
    let config = load_config(&workspace_root());
    for (file, forbidden) in RULES {
        let path = format!("{ROOT}{file}");
        let rules = config
            .capability_rules
            .iter()
            .filter(|rule| rule.root == path)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{path} needs one capability rule");
        assert_eq!(
            rules[0]
                .forbidden
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            *forbidden
        );
        assert!(rules[0].allow.is_empty());
    }
}

#[test]
fn fixture_rejects_policy_runtime_transport_and_public_surface_theft() {
    let (root, _files) = fixture_files("engine_host_group_consumer_capabilities");
    let rules = RULES
        .iter()
        .map(|(file, forbidden)| CapabilityRule {
            root: format!("src/{file}"),
            forbidden: forbidden
                .iter()
                .map(|capability| (*capability).to_owned())
                .collect(),
            allow: Vec::new(),
        })
        .collect::<Vec<_>>();
    let violations = capability_violations(&root, &rules);

    for (file, forbidden) in RULES {
        for capability in *forbidden {
            assert!(
                violations.iter().any(|violation| {
                    violation.contains(file) && violation.contains(capability)
                }),
                "{file} did not reject {capability}: {violations:?}"
            );
        }
    }
}
