//! Checked-in API 40 policy identities for the focused guard test.

pub(super) const ADMIN_ROOT: &str = "crates/kafka-client-engine/src/admin";

macro_rules! core {
    ("hmac.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/hmac.rs"
    };
    ("hmac_test.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/hmac_test.rs"
    };
    ("machine.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/machine.rs"
    };
    ("machine_test.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/machine_test.rs"
    };
    ("model.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/model.rs"
    };
    ("model_test.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/model_test.rs"
    };
    ("outcome.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/outcome.rs"
    };
    ("outcome_test.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/outcome_test.rs"
    };
    ("response.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/response.rs"
    };
    ("response_test.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/response_test.rs"
    };
    ("transition.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/transition.rs"
    };
    ("transition_test.rs") => {
        "crates/kafka-client-core/src/admin/expire_delegation_token/transition_test.rs"
    };
}
macro_rules! engine {
    ("handle.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/handle.rs"
    };
    ("host.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/host.rs"
    };
    ("host/admission.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/host/admission.rs"
    };
    ("host/response.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/host/response.rs"
    };
    ("host/response_test.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/host/response_test.rs"
    };
    ("host/terminal.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/host/terminal.rs"
    };
    ("host_test.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/host_test.rs"
    };
    ("model.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/model.rs"
    };
    ("model_test.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/model_test.rs"
    };
    ("observer.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/observer.rs"
    };
    ("outcome.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/outcome.rs"
    };
    ("outcome_test.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/outcome_test.rs"
    };
    ("result.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/result.rs"
    };
    ("result_test.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/result_test.rs"
    };
    ("shard.rs") => {
        "crates/kafka-client-engine/src/admin/expire_delegation_token/shard.rs"
    };
}
macro_rules! protocol {
    ("prepared.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/expire_delegation_token/prepared.rs"
    };
    ("request.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/expire_delegation_token/request.rs"
    };
    ("request_test.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/expire_delegation_token/request_test.rs"
    };
    ("response.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/expire_delegation_token/response.rs"
    };
    ("response_test.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/expire_delegation_token/response_test.rs"
    };
}
macro_rules! driver {
    ("expire_delegation_token_call.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/expire_delegation_token_call.rs"
    };
    ("expire_delegation_token_submission.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/expire_delegation_token_submission.rs"
    };
    ("expire_delegation_token_submission_test.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/expire_delegation_token_submission_test.rs"
    };
    ("expire_delegation_token_terminal.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/expire_delegation_token_terminal.rs"
    };
    ("expire_delegation_token_terminal_test.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/expire_delegation_token_terminal_test.rs"
    };
}
macro_rules! facade {
    ("builder.rs") => {
        "crates/kafka-client/src/admin/expire_delegation_token/builder.rs"
    };
    ("operation.rs") => {
        "crates/kafka-client/src/admin/expire_delegation_token/operation.rs"
    };
    ("operation_test.rs") => {
        "crates/kafka-client/src/admin/expire_delegation_token/operation_test.rs"
    };
    ("result.rs") => {
        "crates/kafka-client/src/admin/expire_delegation_token/result.rs"
    };
    ("result_test.rs") => {
        "crates/kafka-client/src/admin/expire_delegation_token/result_test.rs"
    };
}
macro_rules! bridge {
    ("operation.rs") => {
        "crates/kafka-client/src/bridge/expire_delegation_token/operation.rs"
    };
    ("request.rs") => {
        "crates/kafka-client/src/bridge/expire_delegation_token/request.rs"
    };
    ("request_test.rs") => {
        "crates/kafka-client/src/bridge/expire_delegation_token/request_test.rs"
    };
    ("result.rs") => {
        "crates/kafka-client/src/bridge/expire_delegation_token/result.rs"
    };
    ("result_test.rs") => {
        "crates/kafka-client/src/bridge/expire_delegation_token/result_test.rs"
    };
}

pub(super) const LINEAR: &[(&str, &str)] = &[
    ("ExpireDelegationTokenHmac", core!("hmac.rs")),
    ("ExpireDelegationTokenPlan", core!("model.rs")),
    ("ExpireDelegationTokenMachine", core!("machine.rs")),
    ("ExpireDelegationTokenTerminal", core!("outcome.rs")),
    ("ExpireDelegationTokenHost", engine!("host.rs")),
    ("ExpireDelegationTokenOperation", engine!("host.rs")),
    ("ExpireDelegationTokenSubmission", engine!("host.rs")),
    ("ExpireDelegationTokenShardOwner", engine!("shard.rs")),
    ("ExpireDelegationTokenCapture", engine!("handle.rs")),
    ("ExpireDelegationTokenObserver", engine!("observer.rs")),
    ("ExpireDelegationTokenAccepted", engine!("handle.rs")),
    ("ExpireDelegationTokenRequest", engine!("model.rs")),
    (
        "PreparedExpireDelegationTokenRequest",
        protocol!("prepared.rs"),
    ),
    (
        "ExpireDelegationTokenCall",
        driver!("expire_delegation_token_call.rs"),
    ),
    (
        "ExpireDelegationTokenRawTerminal",
        driver!("expire_delegation_token_terminal.rs"),
    ),
    (
        "RecoveredExpireDelegationTokenCall",
        driver!("expire_delegation_token_terminal.rs"),
    ),
    ("ExpireDelegationTokenAdminRequest", bridge!("request.rs")),
    ("AdminExpireDelegationToken", bridge!("operation.rs")),
    ("ExpireDelegationTokenBuilder", facade!("builder.rs")),
    ("ExpireDelegationToken", facade!("operation.rs")),
];

pub(super) const MUTATIONS: &[(&str, &str, &[&str])] = &[
    (
        "ExpireDelegationTokenMachine",
        "state",
        &[core!("machine.rs"), core!("transition.rs")],
    ),
    (
        "ExpireDelegationTokenHost",
        "operations",
        &[
            engine!("host.rs"),
            engine!("host/admission.rs"),
            engine!("host/terminal.rs"),
        ],
    ),
    (
        "ExpireDelegationTokenHost",
        "completions",
        &[engine!("host/admission.rs"), engine!("host/terminal.rs")],
    ),
    (
        "ExpireDelegationTokenHost",
        "next_operation_id",
        &[engine!("host/admission.rs")],
    ),
    (
        "ExpireDelegationTokenHost",
        "reclaim_pending",
        &[engine!("host/terminal.rs")],
    ),
    (
        "ExpireDelegationTokenHost",
        "retained_bytes",
        &[engine!("host/admission.rs"), engine!("host/terminal.rs")],
    ),
    (
        "ExpireDelegationTokenHost",
        "accepting",
        &[engine!("host.rs")],
    ),
    (
        "ExpireDelegationTokenHost",
        "health",
        &[engine!("host/admission.rs")],
    ),
    (
        "ExpireDelegationTokenHost",
        "published_bytes",
        &[engine!("host/terminal.rs")],
    ),
    (
        "ExpireDelegationTokenOperation",
        "machine",
        &[
            engine!("host.rs"),
            engine!("host/admission.rs"),
            engine!("host/terminal.rs"),
        ],
    ),
    (
        "ExpireDelegationTokenOperation",
        "remaining_result_bytes",
        &[engine!("host/terminal.rs")],
    ),
    (
        "ExpireDelegationTokenOperation",
        "submission",
        &[engine!("host.rs"), engine!("host/admission.rs")],
    ),
    (
        "ExpireDelegationTokenOperation",
        "handoff",
        &[engine!("host.rs")],
    ),
    (
        "ExpireDelegationTokenOperation",
        "call",
        &[engine!("host.rs"), engine!("host/terminal.rs")],
    ),
    (
        "ExpireDelegationTokenOperation",
        "raw_terminal",
        &[engine!("host/terminal.rs")],
    ),
    (
        "ExpireDelegationTokenOperation",
        "terminal",
        &[
            engine!("host.rs"),
            engine!("host/admission.rs"),
            engine!("host/terminal.rs"),
        ],
    ),
];

pub(super) const MIRRORS: &[(&str, &str)] = &[
    (core!("hmac.rs"), core!("hmac_test.rs")),
    (core!("machine.rs"), core!("machine_test.rs")),
    (core!("model.rs"), core!("model_test.rs")),
    (core!("outcome.rs"), core!("outcome_test.rs")),
    (core!("response.rs"), core!("response_test.rs")),
    (core!("transition.rs"), core!("transition_test.rs")),
    (engine!("host.rs"), engine!("host_test.rs")),
    (
        engine!("host/response.rs"),
        engine!("host/response_test.rs"),
    ),
    (engine!("model.rs"), engine!("model_test.rs")),
    (engine!("outcome.rs"), engine!("outcome_test.rs")),
    (engine!("result.rs"), engine!("result_test.rs")),
    (protocol!("request.rs"), protocol!("request_test.rs")),
    (protocol!("response.rs"), protocol!("response_test.rs")),
    (
        driver!("expire_delegation_token_submission.rs"),
        driver!("expire_delegation_token_submission_test.rs"),
    ),
    (
        driver!("expire_delegation_token_terminal.rs"),
        driver!("expire_delegation_token_terminal_test.rs"),
    ),
    (facade!("operation.rs"), facade!("operation_test.rs")),
    (facade!("result.rs"), facade!("result_test.rs")),
    (bridge!("request.rs"), bridge!("request_test.rs")),
    (bridge!("result.rs"), bridge!("result_test.rs")),
];

pub(super) const METHODS: &[(&str, &str, &[&str])] = &[
    (
        "crates/kafka-client-engine/src",
        "submit_tracked_expire_delegation_token",
        &[driver!("expire_delegation_token_call.rs")],
    ),
    (
        "crates/kafka-client/src",
        "capture_expire_delegation_token",
        &["crates/kafka-client/src/bridge/admin.rs"],
    ),
];

pub(super) const CAPABILITY_ALLOWS: &[(&str, &str)] = &[
    (engine!("host.rs"), "crate::driver"),
    (engine!("host/response.rs"), "crate::driver"),
    (engine!("host/terminal.rs"), "crate::driver"),
];
