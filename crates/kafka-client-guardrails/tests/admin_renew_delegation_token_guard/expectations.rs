//! Checked-in API 39 policy identities for the focused guard test.

pub(super) const ADMIN_ROOT: &str = "crates/kafka-client-engine/src/admin";

macro_rules! core {
    ("hmac.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/hmac.rs"
    };
    ("hmac_test.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/hmac_test.rs"
    };
    ("machine.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/machine.rs"
    };
    ("machine_test.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/machine_test.rs"
    };
    ("model.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/model.rs"
    };
    ("model_test.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/model_test.rs"
    };
    ("outcome.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/outcome.rs"
    };
    ("outcome_test.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/outcome_test.rs"
    };
    ("response.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/response.rs"
    };
    ("response_test.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/response_test.rs"
    };
    ("transition.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/transition.rs"
    };
    ("transition_test.rs") => {
        "crates/kafka-client-core/src/admin/renew_delegation_token/transition_test.rs"
    };
}
macro_rules! engine {
    ("handle.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/handle.rs"
    };
    ("host.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/host.rs"
    };
    ("host/admission.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/host/admission.rs"
    };
    ("host/response.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/host/response.rs"
    };
    ("host/response_test.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/host/response_test.rs"
    };
    ("host/terminal.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/host/terminal.rs"
    };
    ("host/terminal/recovery.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/host/terminal/recovery.rs"
    };
    ("host/terminal/test_support.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/host/terminal/test_support.rs"
    };
    ("host_test.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/host_test.rs"
    };
    ("model.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/model.rs"
    };
    ("model_test.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/model_test.rs"
    };
    ("observer.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/observer.rs"
    };
    ("outcome.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/outcome.rs"
    };
    ("outcome_test.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/outcome_test.rs"
    };
    ("result.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/result.rs"
    };
    ("result_test.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/result_test.rs"
    };
    ("shard.rs") => {
        "crates/kafka-client-engine/src/admin/renew_delegation_token/shard.rs"
    };
}
macro_rules! protocol {
    ("prepared.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/renew_delegation_token/prepared.rs"
    };
    ("request.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/renew_delegation_token/request.rs"
    };
    ("request_test.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/renew_delegation_token/request_test.rs"
    };
    ("response.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/renew_delegation_token/response.rs"
    };
    ("response_test.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/renew_delegation_token/response_test.rs"
    };
}
macro_rules! driver {
    ("renew_delegation_token_call.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/renew_delegation_token_call.rs"
    };
    ("renew_delegation_token_submission.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/renew_delegation_token_submission.rs"
    };
    ("renew_delegation_token_submission_test.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/renew_delegation_token_submission_test.rs"
    };
    ("renew_delegation_token_terminal.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/renew_delegation_token_terminal.rs"
    };
    ("renew_delegation_token_terminal_test.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/renew_delegation_token_terminal_test.rs"
    };
}
macro_rules! facade {
    ("builder.rs") => {
        "crates/kafkars/src/admin/renew_delegation_token/builder.rs"
    };
    ("operation.rs") => {
        "crates/kafkars/src/admin/renew_delegation_token/operation.rs"
    };
    ("operation_test.rs") => {
        "crates/kafkars/src/admin/renew_delegation_token/operation_test.rs"
    };
    ("result.rs") => {
        "crates/kafkars/src/admin/renew_delegation_token/result.rs"
    };
    ("result_test.rs") => {
        "crates/kafkars/src/admin/renew_delegation_token/result_test.rs"
    };
}
macro_rules! bridge {
    ("operation.rs") => {
        "crates/kafkars/src/bridge/renew_delegation_token/operation.rs"
    };
    ("request.rs") => {
        "crates/kafkars/src/bridge/renew_delegation_token/request.rs"
    };
    ("request_test.rs") => {
        "crates/kafkars/src/bridge/renew_delegation_token/request_test.rs"
    };
    ("result.rs") => {
        "crates/kafkars/src/bridge/renew_delegation_token/result.rs"
    };
    ("result_test.rs") => {
        "crates/kafkars/src/bridge/renew_delegation_token/result_test.rs"
    };
}

pub(super) const LINEAR: &[(&str, &str)] = &[
    ("RenewDelegationTokenHmac", core!("hmac.rs")),
    ("RenewDelegationTokenPlan", core!("model.rs")),
    ("RenewDelegationTokenMachine", core!("machine.rs")),
    ("RenewDelegationTokenTerminal", core!("outcome.rs")),
    ("RenewDelegationTokenHost", engine!("host.rs")),
    ("RenewDelegationTokenOperation", engine!("host.rs")),
    ("RenewDelegationTokenSubmission", engine!("host.rs")),
    ("RenewDelegationTokenShardOwner", engine!("shard.rs")),
    ("RenewDelegationTokenCapture", engine!("handle.rs")),
    ("RenewDelegationTokenObserver", engine!("observer.rs")),
    ("RenewDelegationTokenAccepted", engine!("handle.rs")),
    ("RenewDelegationTokenRequest", engine!("model.rs")),
    (
        "PreparedRenewDelegationTokenRequest",
        protocol!("prepared.rs"),
    ),
    (
        "RenewDelegationTokenCall",
        driver!("renew_delegation_token_call.rs"),
    ),
    (
        "RenewDelegationTokenRawTerminal",
        driver!("renew_delegation_token_terminal.rs"),
    ),
    (
        "RecoveredRenewDelegationTokenCall",
        driver!("renew_delegation_token_terminal.rs"),
    ),
    ("RenewDelegationTokenAdminRequest", bridge!("request.rs")),
    ("AdminRenewDelegationToken", bridge!("operation.rs")),
    ("RenewDelegationTokenBuilder", facade!("builder.rs")),
    ("RenewDelegationToken", facade!("operation.rs")),
];

pub(super) const MUTATIONS: &[(&str, &str, &[&str])] = &[
    (
        "RenewDelegationTokenMachine",
        "state",
        &[core!("machine.rs"), core!("transition.rs")],
    ),
    (
        "RenewDelegationTokenHost",
        "operations",
        &[
            engine!("host.rs"),
            engine!("host/admission.rs"),
            engine!("host/terminal.rs"),
            engine!("host/terminal/recovery.rs"),
        ],
    ),
    (
        "RenewDelegationTokenHost",
        "completions",
        &[engine!("host/admission.rs"), engine!("host/terminal.rs")],
    ),
    (
        "RenewDelegationTokenHost",
        "next_operation_id",
        &[engine!("host/admission.rs")],
    ),
    (
        "RenewDelegationTokenHost",
        "reclaim_pending",
        &[engine!("host/terminal.rs")],
    ),
    (
        "RenewDelegationTokenHost",
        "retained_bytes",
        &[engine!("host/admission.rs"), engine!("host/terminal.rs")],
    ),
    (
        "RenewDelegationTokenHost",
        "accepting",
        &[engine!("host.rs")],
    ),
    (
        "RenewDelegationTokenHost",
        "health",
        &[engine!("host/admission.rs")],
    ),
    (
        "RenewDelegationTokenHost",
        "published_bytes",
        &[engine!("host/terminal.rs")],
    ),
    (
        "RenewDelegationTokenOperation",
        "machine",
        &[
            engine!("host.rs"),
            engine!("host/admission.rs"),
            engine!("host/terminal.rs"),
        ],
    ),
    (
        "RenewDelegationTokenOperation",
        "remaining_result_bytes",
        &[engine!("host/terminal.rs")],
    ),
    (
        "RenewDelegationTokenOperation",
        "submission",
        &[engine!("host.rs"), engine!("host/admission.rs")],
    ),
    (
        "RenewDelegationTokenOperation",
        "raw_terminal",
        &[engine!("host/terminal.rs")],
    ),
    (
        "RenewDelegationTokenOperation",
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
        driver!("renew_delegation_token_submission.rs"),
        driver!("renew_delegation_token_submission_test.rs"),
    ),
    (
        driver!("renew_delegation_token_terminal.rs"),
        driver!("renew_delegation_token_terminal_test.rs"),
    ),
    (facade!("operation.rs"), facade!("operation_test.rs")),
    (facade!("result.rs"), facade!("result_test.rs")),
    (bridge!("request.rs"), bridge!("request_test.rs")),
    (bridge!("result.rs"), bridge!("result_test.rs")),
];

pub(super) const METHODS: &[(&str, &str, &[&str])] = &[
    (
        "crates/kafka-client-engine/src",
        "submit_tracked_renew_delegation_token",
        &[driver!("renew_delegation_token_call.rs")],
    ),
    (
        "crates/kafkars/src",
        "capture_renew_delegation_token",
        &["crates/kafkars/src/bridge/admin.rs"],
    ),
];

pub(super) const CAPABILITY_ALLOWS: &[(&str, &str)] = &[
    (engine!("host.rs"), "crate::driver"),
    (engine!("host/response.rs"), "crate::driver"),
    (engine!("host/terminal/test_support.rs"), "crate::driver"),
    (engine!("host_test.rs"), "crate::driver"),
];
