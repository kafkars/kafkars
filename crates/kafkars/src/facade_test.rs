//! Tests for the intentionally small crate-root navigation surface.

const CRATE_FACADE: &str = include_str!("lib.rs");

#[test]
fn root_exports_stay_small_and_domain_owned() {
    let modules = CRATE_FACADE
        .lines()
        .filter_map(|line| {
            line.strip_prefix("pub mod ")
                .map(|module| module.trim_end_matches(';'))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        modules,
        [
            "admin",
            "client",
            "consumer",
            "error",
            "metrics",
            "producer",
            "security",
            "topic",
            "transaction",
        ]
    );

    let exports = CRATE_FACADE
        .lines()
        .filter_map(|line| {
            line.strip_prefix("pub use ")
                .map(|export| export.trim_end_matches(';'))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        exports,
        [
            "admin::Admin",
            "client::Client",
            "consumer::Consumer",
            "error::{Error, Result}",
            "producer::Producer",
        ]
    );
    assert!(CRATE_FACADE.contains("pub(crate) use exports::{"));
    assert!(!CRATE_FACADE.contains("use exports::*;"));
    assert!(!CRATE_FACADE.contains("pub use exports::*;"));
}
