//! Target root exposing bounded, escaped, and generated module paths.

#[path = "../escaped.rs"]
mod escaped;
#[cfg_attr(any(), path = "reachable.rs")]
mod conditional;
mod generated;
mod indirect_include;
mod macro_module;
mod macro_generated;
mod macro_scope_launder;
mod reachable;
mod safe_macros;
mod shadowed_builtin;
mod shadowed_core_extern;
mod shadowed_import;
mod shadowed_raw_root;
mod shadowed_syn;
#[cfg(target_os = "guardrail-fixture")]
mod target_specific;
mod trusted_qualified;
