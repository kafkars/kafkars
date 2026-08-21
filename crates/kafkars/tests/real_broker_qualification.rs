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
        .unwrap_or_else(|error| panic!("real-broker pull-request smoke failed: {error}"));
}
