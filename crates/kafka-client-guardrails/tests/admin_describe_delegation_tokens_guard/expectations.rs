//! Checked-in API 41 policy identities for the focused guard test.

pub(super) const ADMIN_ROOT: &str = "crates/kafka-client-engine/src/admin";

macro_rules! core {
    ("machine.rs") => {
        "crates/kafka-client-core/src/admin/describe_delegation_tokens/machine.rs"
    };
    ("model.rs") => {
        "crates/kafka-client-core/src/admin/describe_delegation_tokens/model.rs"
    };
    ("model_test.rs") => {
        "crates/kafka-client-core/src/admin/describe_delegation_tokens/model_test.rs"
    };
    ("outcome.rs") => {
        "crates/kafka-client-core/src/admin/describe_delegation_tokens/outcome.rs"
    };
    ("response.rs") => {
        "crates/kafka-client-core/src/admin/describe_delegation_tokens/response.rs"
    };
    ("response_test.rs") => {
        "crates/kafka-client-core/src/admin/describe_delegation_tokens/response_test.rs"
    };
    ("transition.rs") => {
        "crates/kafka-client-core/src/admin/describe_delegation_tokens/transition.rs"
    };
    ("transition_test.rs") => {
        "crates/kafka-client-core/src/admin/describe_delegation_tokens/transition_test.rs"
    };
}
macro_rules! engine {
    ("handle.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/handle.rs"
    };
    ("host.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/host.rs"
    };
    ("host/admission.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/host/admission.rs"
    };
    ("host/response.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/host/response.rs"
    };
    ("host/response_test.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/host/response_test.rs"
    };
    ("host/terminal.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/host/terminal.rs"
    };
    ("host_test.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/host_test.rs"
    };
    ("model.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/model.rs"
    };
    ("model_test.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/model_test.rs"
    };
    ("observer.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/observer.rs"
    };
    ("outcome.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/outcome.rs"
    };
    ("outcome_test.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/outcome_test.rs"
    };
    ("result.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/result.rs"
    };
    ("shard.rs") => {
        "crates/kafka-client-engine/src/admin/describe_delegation_tokens/shard.rs"
    };
}
macro_rules! protocol {
    ("model.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/describe_delegation_tokens/model.rs"
    };
    ("prepared.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/describe_delegation_tokens/prepared.rs"
    };
    ("request.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/describe_delegation_tokens/request.rs"
    };
    ("request_test.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/describe_delegation_tokens/request_test.rs"
    };
    ("response.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/describe_delegation_tokens/response.rs"
    };
    ("response_test.rs") => {
        "crates/kafka-client-engine/src/protocol/admin/describe_delegation_tokens/response_test.rs"
    };
}
macro_rules! driver {
    ("describe_delegation_tokens_call.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/describe_delegation_tokens_call.rs"
    };
    ("describe_delegation_tokens_submission.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/describe_delegation_tokens_submission.rs"
    };
    ("describe_delegation_tokens_submission_test.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/describe_delegation_tokens_submission_test.rs"
    };
    ("describe_delegation_tokens_terminal.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/describe_delegation_tokens_terminal.rs"
    };
    ("describe_delegation_tokens_terminal_test.rs") => {
        "crates/kafka-client-engine/src/driver/rpc/describe_delegation_tokens_terminal_test.rs"
    };
}
macro_rules! facade {
    ("builder.rs") => {
        "crates/kafka-client/src/admin/describe_delegation_tokens/builder.rs"
    };
    ("operation.rs") => {
        "crates/kafka-client/src/admin/describe_delegation_tokens/operation.rs"
    };
    ("operation_test.rs") => {
        "crates/kafka-client/src/admin/describe_delegation_tokens/operation_test.rs"
    };
    ("result.rs") => {
        "crates/kafka-client/src/admin/describe_delegation_tokens/result.rs"
    };
    ("result_test.rs") => {
        "crates/kafka-client/src/admin/describe_delegation_tokens/result_test.rs"
    };
}
macro_rules! bridge {
    ("operation.rs") => {
        "crates/kafka-client/src/bridge/describe_delegation_tokens/operation.rs"
    };
    ("request.rs") => {
        "crates/kafka-client/src/bridge/describe_delegation_tokens/request.rs"
    };
    ("request_test.rs") => {
        "crates/kafka-client/src/bridge/describe_delegation_tokens/request_test.rs"
    };
    ("result.rs") => {
        "crates/kafka-client/src/bridge/describe_delegation_tokens/result.rs"
    };
    ("result_test.rs") => {
        "crates/kafka-client/src/bridge/describe_delegation_tokens/result_test.rs"
    };
}

pub(super) const LINEAR: &[(&str, &str)] = &[
    ("DescribeDelegationTokensMachine", core!("machine.rs")),
    ("DescribeDelegationTokensTerminal", core!("outcome.rs")),
    ("DescribeDelegationTokensHost", engine!("host.rs")),
    ("DescribeDelegationTokensOperation", engine!("host.rs")),
    ("DescribeDelegationTokensSubmission", engine!("host.rs")),
    ("DescribeDelegationTokensShardOwner", engine!("shard.rs")),
    ("DescribeDelegationTokensCapture", engine!("handle.rs")),
    ("DescribeDelegationTokensObserver", engine!("observer.rs")),
    ("DescribeDelegationTokensAccepted", engine!("handle.rs")),
    ("DescribeDelegationTokenHmac", engine!("result.rs")),
    ("DescribedDelegationToken", engine!("result.rs")),
    ("DescribeDelegationTokensOutcome", engine!("outcome.rs")),
    (
        "PreparedDescribeDelegationTokensRequest",
        protocol!("prepared.rs"),
    ),
    ("NormalizedDescribedDelegationToken", protocol!("model.rs")),
    (
        "NormalizedDescribeDelegationTokensResponse",
        protocol!("model.rs"),
    ),
    (
        "DescribeDelegationTokensCall",
        driver!("describe_delegation_tokens_call.rs"),
    ),
    (
        "DescribeDelegationTokensRawTerminal",
        driver!("describe_delegation_tokens_terminal.rs"),
    ),
    (
        "RecoveredDescribeDelegationTokensCall",
        driver!("describe_delegation_tokens_terminal.rs"),
    ),
    (
        "DescribeDelegationTokensAdminRequest",
        bridge!("request.rs"),
    ),
    ("AdminDescribeDelegationTokens", bridge!("operation.rs")),
    ("DescribeDelegationTokensBuilder", facade!("builder.rs")),
    ("DescribeDelegationTokens", facade!("operation.rs")),
];

pub(super) const MUTATIONS: &[(&str, &str, &[&str])] = &[
    (
        "DescribeDelegationTokensMachine",
        "state",
        &[core!("machine.rs"), core!("transition.rs")],
    ),
    (
        "DescribeDelegationTokensHost",
        "operations",
        &[
            engine!("host.rs"),
            engine!("host/admission.rs"),
            engine!("host/terminal.rs"),
        ],
    ),
    (
        "DescribeDelegationTokensHost",
        "completions",
        &[engine!("host/admission.rs"), engine!("host/terminal.rs")],
    ),
    (
        "DescribeDelegationTokensHost",
        "next_operation_id",
        &[engine!("host/admission.rs")],
    ),
    (
        "DescribeDelegationTokensHost",
        "reclaim_pending",
        &[engine!("host/terminal.rs")],
    ),
    (
        "DescribeDelegationTokensHost",
        "retained_bytes",
        &[engine!("host/admission.rs"), engine!("host/terminal.rs")],
    ),
    (
        "DescribeDelegationTokensHost",
        "accepting",
        &[engine!("host.rs")],
    ),
    (
        "DescribeDelegationTokensHost",
        "health",
        &[engine!("host/admission.rs")],
    ),
    (
        "DescribeDelegationTokensHost",
        "published_bytes",
        &[engine!("host/terminal.rs")],
    ),
    (
        "DescribeDelegationTokensOperation",
        "machine",
        &[
            engine!("host.rs"),
            engine!("host/admission.rs"),
            engine!("host/terminal.rs"),
        ],
    ),
    (
        "DescribeDelegationTokensOperation",
        "remaining_result_bytes",
        &[engine!("host/terminal.rs")],
    ),
    (
        "DescribeDelegationTokensOperation",
        "submission",
        &[engine!("host.rs"), engine!("host/admission.rs")],
    ),
    (
        "DescribeDelegationTokensOperation",
        "handoff",
        &[engine!("host.rs")],
    ),
    (
        "DescribeDelegationTokensOperation",
        "call",
        &[engine!("host.rs"), engine!("host/terminal.rs")],
    ),
    (
        "DescribeDelegationTokensOperation",
        "raw_terminal",
        &[engine!("host/terminal.rs")],
    ),
    (
        "DescribeDelegationTokensOperation",
        "terminal",
        &[
            engine!("host.rs"),
            engine!("host/admission.rs"),
            engine!("host/terminal.rs"),
        ],
    ),
];

pub(super) const MIRRORS: &[(&str, &str)] = &[
    (core!("model.rs"), core!("model_test.rs")),
    (core!("response.rs"), core!("response_test.rs")),
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
        driver!("describe_delegation_tokens_submission.rs"),
        driver!("describe_delegation_tokens_submission_test.rs"),
    ),
    (
        driver!("describe_delegation_tokens_terminal.rs"),
        driver!("describe_delegation_tokens_terminal_test.rs"),
    ),
    (facade!("operation.rs"), facade!("operation_test.rs")),
    (facade!("result.rs"), facade!("result_test.rs")),
    (bridge!("request.rs"), bridge!("request_test.rs")),
    (bridge!("result.rs"), bridge!("result_test.rs")),
];

pub(super) const METHODS: &[(&str, &str, &[&str])] = &[
    (
        "crates/kafka-client-engine/src",
        "submit_tracked_describe_delegation_tokens",
        &[driver!("describe_delegation_tokens_call.rs")],
    ),
    (
        "crates/kafka-client/src",
        "capture_describe_delegation_tokens",
        &["crates/kafka-client/src/bridge/admin.rs"],
    ),
];

pub(super) const CAPABILITY_ALLOWS: &[(&str, &str)] = &[
    (engine!("host.rs"), "crate::driver"),
    (engine!("host/response.rs"), "crate::driver"),
    (engine!("host/terminal.rs"), "crate::driver"),
];
