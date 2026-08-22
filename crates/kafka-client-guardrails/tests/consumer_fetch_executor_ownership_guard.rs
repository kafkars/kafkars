//! Ownership, capability, and machine-binding guards for direct Fetch execution.
#[path = "consumer_fetch_executor_ownership_guard/policy.rs"]
mod policy;
mod support;

use policy::{CAPABILITY_ALLOWS, FORBIDDEN};
use support::{
    LinearOwner, MutationOwner, fixture_files, linear_violations, load_config, mutation_violations,
    workspace_root,
};

const EXECUTION: &str = "crates/kafka-client-engine/src/consumer/fetch_execution";
const ADMISSION: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/admission.rs";
const APPLY: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/apply.rs";
const BROKER_CLOSE: &str =
    "crates/kafka-client-engine/src/consumer/fetch_execution/broker_close.rs";
const BROKER_EXECUTION: &str =
    "crates/kafka-client-engine/src/consumer/fetch_execution/broker_execution.rs";
const BROKER_ROUTE_POLL: &str =
    "crates/kafka-client-engine/src/consumer/fetch_execution/broker_route_poll.rs";
const BROKER_MAINTENANCE: &str =
    "crates/kafka-client-engine/src/consumer/fetch_execution/broker_maintenance.rs";
const BROKER_MAINTENANCE_SETTLEMENT: &str =
    "crates/kafka-client-engine/src/consumer/fetch_execution/broker_maintenance_settlement.rs";
const BROKER_MAINTENANCE_STATE: &str =
    "crates/kafka-client-engine/src/consumer/fetch_execution/broker_maintenance_state.rs";
const BROKER_SUBMISSION: &str =
    "crates/kafka-client-engine/src/consumer/fetch_execution/broker_submission.rs";
const CONTROL: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/control.rs";
const DEADLINE: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/deadline.rs";
const DELIVERY: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/delivery.rs";
const EXECUTOR: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/executor.rs";
const FAULT: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/fault.rs";
const LEADER_RETRY: &str =
    "crates/kafka-client-engine/src/consumer/fetch_execution/leader_retry.rs";
const PREPARED: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/prepared.rs";
const SETTLEMENT: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/settlement.rs";
const TERMINAL: &str = "crates/kafka-client-engine/src/consumer/fetch_execution/terminal.rs";

const LINEAR: &[(&str, &str)] = &[
    ("FetchAttemptDeadline", DEADLINE),
    ("PreparedFetchExecution", PREPARED),
    ("FetchSubmission", ADMISSION),
    ("ActiveFetchReservation", EXECUTOR),
    ("BrokerSessionMaintenance", BROKER_MAINTENANCE_STATE),
    ("ExecutorSeal", EXECUTOR),
    ("DirectFetchExecutor", EXECUTOR),
    ("RetainedFetchFault", FAULT),
    ("FetchReclaimFailure", FAULT),
    ("FetchShutdownRecovery", FAULT),
    ("FetchTerminalFact", TERMINAL),
];

const MUTATIONS: &[(&str, &str, &[&str])] = &[
    (
        "DirectFetchExecutor",
        "calls",
        &[
            ADMISSION,
            APPLY,
            CONTROL,
            DELIVERY,
            EXECUTOR,
            SETTLEMENT,
            BROKER_CLOSE,
        ],
    ),
    (
        "DirectFetchExecutor",
        "store",
        &[
            ADMISSION,
            APPLY,
            CONTROL,
            DELIVERY,
            EXECUTOR,
            LEADER_RETRY,
            TERMINAL,
            BROKER_CLOSE,
            BROKER_SUBMISSION,
        ],
    ),
    (
        "DirectFetchExecutor",
        "active",
        &[ADMISSION, EXECUTOR, BROKER_SUBMISSION],
    ),
    (
        "DirectFetchExecutor",
        "fault",
        &[
            ADMISSION,
            APPLY,
            CONTROL,
            DELIVERY,
            EXECUTOR,
            LEADER_RETRY,
            SETTLEMENT,
            TERMINAL,
            BROKER_CLOSE,
            BROKER_EXECUTION,
            BROKER_ROUTE_POLL,
            BROKER_MAINTENANCE,
            BROKER_MAINTENANCE_SETTLEMENT,
            BROKER_SUBMISSION,
        ],
    ),
    (
        "DirectFetchExecutor",
        "broker_maintenance",
        &[
            BROKER_CLOSE,
            BROKER_EXECUTION,
            BROKER_MAINTENANCE,
            BROKER_MAINTENANCE_SETTLEMENT,
            BROKER_MAINTENANCE_STATE,
            EXECUTOR,
        ],
    ),
    (
        "DirectFetchExecutor",
        "broker_session_policy",
        &[BROKER_CLOSE, BROKER_MAINTENANCE, EXECUTOR],
    ),
];

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one policy test compares every checked-in ownership and capability rule"
)]
fn checked_in_executor_policy_is_exact() {
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
    let capabilities = config
        .capability_rules
        .iter()
        .filter(|rule| rule.root == EXECUTION)
        .collect::<Vec<_>>();
    assert_eq!(capabilities.len(), 1);
    assert_eq!(
        capabilities[0]
            .forbidden
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        FORBIDDEN,
    );
    assert_eq!(
        capabilities[0]
            .allow
            .iter()
            .map(|allow| {
                (
                    allow.path.rsplit('/').next().unwrap_or_default(),
                    allow.capability.as_str(),
                )
            })
            .collect::<Vec<_>>(),
        CAPABILITY_ALLOWS,
    );
    assert!(
        capabilities[0]
            .allow
            .iter()
            .all(|allow| !allow.reason.trim().is_empty())
    );

    let constructors = config
        .call_capabilities
        .iter()
        .filter(|rule| rule.call == "DirectFetchExecutor::create_unbound")
        .collect::<Vec<_>>();
    assert_eq!(constructors.len(), 1);
    assert_eq!(
        constructors[0].allowed_paths,
        [
            "crates/kafka-client-engine/src/consumer/assigned_owner.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/owner_build.rs",
        ]
    );
    let deadline_constructors = config
        .call_capabilities
        .iter()
        .filter(|rule| rule.call == "FetchAttemptDeadline::capture_for_fetch")
        .collect::<Vec<_>>();
    assert_eq!(deadline_constructors.len(), 1);
    assert_eq!(
        deadline_constructors[0].allowed_paths,
        [
            "crates/kafka-client-engine/src/consumer/assigned_owner_effect.rs",
            "crates/kafka-client-engine/src/consumer/group/classic_group_fetch/prepare.rs",
        ]
    );

    for (method, execution_paths) in [
        ("try_reserve", vec![ADMISSION, EXECUTOR, BROKER_SUBMISSION]),
        (
            "retained_count",
            vec![EXECUTOR, BROKER_CLOSE, BROKER_MAINTENANCE],
        ),
    ] {
        let rules = config
            .method_capabilities
            .iter()
            .filter(|rule| rule.method == method)
            .collect::<Vec<_>>();
        assert_eq!(rules.len(), 1, "{method} needs one method capability");
        assert_eq!(
            rules[0]
                .allowed_paths
                .iter()
                .map(String::as_str)
                .filter(|path| path.starts_with(EXECUTION))
                .collect::<Vec<_>>(),
            execution_paths,
        );
    }
}

#[test]
fn fixture_rejects_executor_duplication_and_foreign_mutation() {
    let (root, files) = fixture_files("consumer_fetch_executor_ownership");
    let linear = LINEAR
        .iter()
        .map(|(owner_type, _)| LinearOwner {
            owner_type: (*owner_type).into(),
            path: "src/linear_intruder.rs".into(),
        })
        .collect::<Vec<_>>();
    let violations = linear_violations(&root, &files, &linear);
    for (owner_type, _) in LINEAR {
        for derived in ["derives Clone", "derives Copy"] {
            assert!(violations.iter().any(|violation| {
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
    let violations = mutation_violations(&root, &files, &mutations);
    for (owner_type, field, _) in MUTATIONS {
        assert!(violations.iter().any(|violation| {
            violation.contains("mutation_intruder.rs")
                && violation.contains(owner_type)
                && violation.contains(field)
        }));
    }
}
