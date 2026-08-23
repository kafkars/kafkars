//! Maintained real-broker qualification scenarios over the public Rust facade.

#[path = "real_broker_qualification/mod.rs"]
mod qualification;
#[allow(
    dead_code,
    reason = "this isolated target compiles the complete shared broker helper surface"
)]
#[path = "real_broker_support/mod.rs"]
mod real_broker_support;

#[test]
#[ignore = "requires a qualification-managed mutable Kafka cluster"]
fn pull_request_smoke() {
    qualification::run_pull_request_smoke()
        .unwrap_or_else(|error| panic!("real-broker pull-request smoke failed: {error:?}"));
}

#[test]
#[ignore = "requires a qualification-managed three-broker Kafka cluster"]
fn nightly_matrix() {
    qualification::run_nightly_matrix()
        .unwrap_or_else(|error| panic!("real-broker nightly matrix failed: {error:?}"));
}

#[test]
#[ignore = "requires a qualification-managed three-broker Kafka cluster"]
fn classic_matrix() {
    qualification::run_classic_matrix()
        .unwrap_or_else(|error| panic!("real-broker classic matrix failed: {error:?}"));
}

#[test]
#[ignore = "requires a qualification-managed share-enabled Kafka cluster"]
fn share_matrix() {
    qualification::run_share_matrix()
        .unwrap_or_else(|error| panic!("real-broker share matrix failed: {error:?}"));
}
