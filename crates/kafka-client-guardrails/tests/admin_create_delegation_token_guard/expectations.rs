//! Checked-in API 38 policy identities for the focused guard test.

pub(super) const ADMIN_ROOT: &str = "crates/kafka-client-engine/src/admin";

macro_rules! core {
    ("machine.rs") => {
        "crates/kafka-client-core/src/admin/create_delegation_token/machine.rs"
    };
    ("machine_test.rs") => {
        "crates/kafka-client-core/src/admin/create_delegation_token/machine_test.rs"
    };
    ("model.rs") => {
        "crates/kafka-client-core/src/admin/create_delegation_token/model.rs"
    };
    ("model_test.rs") => {
        "crates/kafka-client-core/src/admin/create_delegation_token/model_test.rs"
    };
    ("outcome.rs") => {
        "crates/kafka-client-core/src/admin/create_delegation_token/outcome.rs"
    };
    ("outcome_test.rs") => {
        "crates/kafka-client-core/src/admin/create_delegation_token/outcome_test.rs"
    };
    ("transition.rs") => {
        "crates/kafka-client-core/src/admin/create_delegation_token/transition.rs"
    };
    ("transition_test.rs") => {
        "crates/kafka-client-core/src/admin/create_delegation_token/transition_test.rs"
    };
}
macro_rules! engine {
    ("handle.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/handle.rs"
    };
    ("host.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/host.rs"
    };
    ("host/admission.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/host/admission.rs"
    };
    ("host/response.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/host/response.rs"
    };
    ("host/response_test.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/host/response_test.rs"
    };
    ("host/terminal.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/host/terminal.rs"
    };
    ("host/terminal/recovery.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/host/terminal/recovery.rs"
    };
    ("host/terminal/recovery_test.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/host/terminal/recovery_test.rs"
    };
    ("host/terminal/test_support.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/host/terminal/test_support.rs"
    };
    ("host_test.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/host_test.rs"
    };
    ("model.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/model.rs"
    };
    ("model_test.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/model_test.rs"
    };
    ("observer.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/observer.rs"
    };
    ("outcome.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/outcome.rs"
    };
    ("outcome_test.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/outcome_test.rs"
    };
    ("result.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/result.rs"
    };
    ("shard.rs") => {
        "crates/kafka-client-engine/src/admin/create_delegation_token/shard.rs"
    };
}
macro_rules! protocol {
    ("model.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/create_delegation_token/model.rs"
    };
    ("prepared.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/create_delegation_token/prepared.rs"
    };
    ("request.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/create_delegation_token/request.rs"
    };
    ("request_test.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/create_delegation_token/request_test.rs"
    };
    ("response.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/create_delegation_token/response.rs"
    };
    ("response_test.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/create_delegation_token/response_test.rs"
    };
}
macro_rules! driver {
    ("create_delegation_token_call.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/create_delegation_token_call.rs"
    };
    ("create_delegation_token_submission.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/create_delegation_token_submission.rs"
    };
    ("create_delegation_token_submission_test.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/create_delegation_token_submission_test.rs"
    };
    ("create_delegation_token_terminal.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/create_delegation_token_terminal.rs"
    };
}
macro_rules! facade {
    ("builder.rs") => {
        "crates/kafkars/src/admin/create_delegation_token/builder.rs"
    };
    ("hmac.rs") => {
        "crates/kafkars/src/admin/create_delegation_token/hmac.rs"
    };
    ("hmac_test.rs") => {
        "crates/kafkars/src/admin/create_delegation_token/hmac_test.rs"
    };
    ("operation.rs") => {
        "crates/kafkars/src/admin/create_delegation_token/operation.rs"
    };
    ("operation_test.rs") => {
        "crates/kafkars/src/admin/create_delegation_token/operation_test.rs"
    };
    ("principal.rs") => {
        "crates/kafkars/src/admin/create_delegation_token/principal.rs"
    };
    ("principal_test.rs") => {
        "crates/kafkars/src/admin/create_delegation_token/principal_test.rs"
    };
    ("result.rs") => {
        "crates/kafkars/src/admin/create_delegation_token/result.rs"
    };
    ("result_test.rs") => {
        "crates/kafkars/src/admin/create_delegation_token/result_test.rs"
    };
    ("token.rs") => {
        "crates/kafkars/src/admin/create_delegation_token/token.rs"
    };
    ("token_test.rs") => {
        "crates/kafkars/src/admin/create_delegation_token/token_test.rs"
    };
}
macro_rules! bridge {
    ("operation.rs") => {
        "crates/kafkars/src/bridge/create_delegation_token/operation.rs"
    };
    ("request.rs") => {
        "crates/kafkars/src/bridge/create_delegation_token/request.rs"
    };
    ("request_test.rs") => {
        "crates/kafkars/src/bridge/create_delegation_token/request_test.rs"
    };
    ("result.rs") => {
        "crates/kafkars/src/bridge/create_delegation_token/result.rs"
    };
    ("result_test.rs") => {
        "crates/kafkars/src/bridge/create_delegation_token/result_test.rs"
    };
}

pub(super) const LINEAR: &[(&str, &str)] = &[
    ("CreateDelegationTokenMachine", core!("machine.rs")),
    ("CreateDelegationTokenTerminal", core!("outcome.rs")),
    ("CreateDelegationTokenHost", engine!("host.rs")),
    ("CreateDelegationTokenOperation", engine!("host.rs")),
    ("CreateDelegationTokenSubmission", engine!("host.rs")),
    ("CreateDelegationTokenShardOwner", engine!("shard.rs")),
    ("CreateDelegationTokenCapture", engine!("handle.rs")),
    ("CreateDelegationTokenObserver", engine!("observer.rs")),
    ("CreateDelegationTokenAccepted", engine!("handle.rs")),
    ("CreateDelegationTokenHmac", engine!("result.rs")),
    ("CreatedDelegationToken", engine!("result.rs")),
    ("CreateDelegationTokenOutcome", engine!("outcome.rs")),
    (
        "PreparedCreateDelegationTokenRequest",
        protocol!("prepared.rs"),
    ),
    ("NormalizedDelegationToken", protocol!("model.rs")),
    (
        "NormalizedCreateDelegationTokenResponse",
        protocol!("model.rs"),
    ),
    (
        "CreateDelegationTokenCall",
        driver!("create_delegation_token_call.rs"),
    ),
    (
        "CreateDelegationTokenRawTerminal",
        driver!("create_delegation_token_terminal.rs"),
    ),
    (
        "RecoveredCreateDelegationTokenCall",
        driver!("create_delegation_token_terminal.rs"),
    ),
    ("CreateDelegationTokenAdminRequest", bridge!("request.rs")),
    ("AdminCreateDelegationToken", bridge!("operation.rs")),
    ("CreateDelegationTokenBuilder", facade!("builder.rs")),
    ("CreateDelegationToken", facade!("operation.rs")),
];

pub(super) const MUTATIONS: &[(&str, &str, &[&str])] = &[
    (
        "CreateDelegationTokenMachine",
        "state",
        &[core!("machine.rs"), core!("transition.rs")],
    ),
    (
        "CreateDelegationTokenHost",
        "operations",
        &[
            engine!("host.rs"),
            engine!("host/admission.rs"),
            engine!("host/terminal.rs"),
            engine!("host/terminal/recovery.rs"),
        ],
    ),
    (
        "CreateDelegationTokenHost",
        "completions",
        &[engine!("host/admission.rs"), engine!("host/terminal.rs")],
    ),
    (
        "CreateDelegationTokenHost",
        "next_operation_id",
        &[engine!("host/admission.rs")],
    ),
    (
        "CreateDelegationTokenHost",
        "reclaim_pending",
        &[engine!("host/terminal.rs")],
    ),
    (
        "CreateDelegationTokenHost",
        "retained_bytes",
        &[engine!("host/admission.rs"), engine!("host/terminal.rs")],
    ),
    (
        "CreateDelegationTokenHost",
        "accepting",
        &[engine!("host.rs")],
    ),
    (
        "CreateDelegationTokenHost",
        "health",
        &[engine!("host/admission.rs")],
    ),
    (
        "CreateDelegationTokenHost",
        "published_bytes",
        &[engine!("host/terminal.rs")],
    ),
    (
        "CreateDelegationTokenOperation",
        "machine",
        &[
            engine!("host.rs"),
            engine!("host/admission.rs"),
            engine!("host/terminal.rs"),
        ],
    ),
    (
        "CreateDelegationTokenOperation",
        "remaining_result_bytes",
        &[engine!("host/terminal.rs")],
    ),
    (
        "CreateDelegationTokenOperation",
        "submission",
        &[engine!("host.rs"), engine!("host/admission.rs")],
    ),
    (
        "CreateDelegationTokenOperation",
        "handoff",
        &[engine!("host.rs")],
    ),
    (
        "CreateDelegationTokenOperation",
        "call",
        &[engine!("host.rs"), engine!("host/terminal.rs")],
    ),
    (
        "CreateDelegationTokenOperation",
        "raw_terminal",
        &[engine!("host/terminal.rs")],
    ),
    (
        "CreateDelegationTokenOperation",
        "terminal",
        &[
            engine!("host.rs"),
            engine!("host/admission.rs"),
            engine!("host/terminal.rs"),
        ],
    ),
];

pub(super) const MIRRORS: &[(&str, &str)] = &[
    (core!("machine.rs"), core!("machine_test.rs")),
    (core!("model.rs"), core!("model_test.rs")),
    (core!("outcome.rs"), core!("outcome_test.rs")),
    (core!("transition.rs"), core!("transition_test.rs")),
    (engine!("host.rs"), engine!("host_test.rs")),
    (
        engine!("host/response.rs"),
        engine!("host/response_test.rs"),
    ),
    (engine!("model.rs"), engine!("model_test.rs")),
    (engine!("outcome.rs"), engine!("outcome_test.rs")),
    (protocol!("request.rs"), protocol!("request_test.rs")),
    (protocol!("response.rs"), protocol!("response_test.rs")),
    (
        driver!("create_delegation_token_submission.rs"),
        driver!("create_delegation_token_submission_test.rs"),
    ),
    (facade!("hmac.rs"), facade!("hmac_test.rs")),
    (facade!("principal.rs"), facade!("principal_test.rs")),
    (facade!("operation.rs"), facade!("operation_test.rs")),
    (facade!("result.rs"), facade!("result_test.rs")),
    (facade!("token.rs"), facade!("token_test.rs")),
    (bridge!("request.rs"), bridge!("request_test.rs")),
    (bridge!("result.rs"), bridge!("result_test.rs")),
];

pub(super) const METHODS: &[(&str, &str, &[&str])] = &[
    (
        "crates/kafka-client-engine/src",
        "submit_tracked_create_delegation_token",
        &[driver!("create_delegation_token_call.rs")],
    ),
    (
        "crates/kafkars/src",
        "capture_create_delegation_token",
        &["crates/kafkars/src/bridge/admin.rs"],
    ),
];

pub(super) const CAPABILITY_ALLOWS: &[(&str, &str)] = &[
    (engine!("host.rs"), "crate::driver"),
    (engine!("host/response.rs"), "crate::driver"),
    (engine!("host/terminal/recovery_test.rs"), "crate::driver"),
    (engine!("host/terminal/test_support.rs"), "crate::driver"),
];
