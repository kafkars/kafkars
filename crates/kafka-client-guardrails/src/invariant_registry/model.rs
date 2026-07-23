//! Strict schema and parser for the normative invariant registry.

use serde::Deserialize;

pub(super) const SUPPORTED_SCHEMA: u32 = 1;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Registry {
    pub(super) schema: u32,
    #[serde(rename = "invariant")]
    pub(super) invariants: Vec<Invariant>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Invariant {
    pub(super) id: String,
    pub(super) title: String,
    pub(super) statement: String,
    pub(super) status: String,
    #[serde(default)]
    pub(super) evidence: Vec<String>,
    pub(super) milestone: Option<String>,
}

pub(super) fn parse(source: &str) -> Result<Registry, String> {
    toml::from_str(source).map_err(|error| error.to_string())
}
