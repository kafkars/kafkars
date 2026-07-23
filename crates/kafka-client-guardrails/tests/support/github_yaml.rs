//! Constrained structural YAML parsing for the repository's GitHub documents.

mod model;
mod parser;
mod syntax;

pub(crate) use model::{YamlNode, entry};
pub(crate) use parser::parse;
