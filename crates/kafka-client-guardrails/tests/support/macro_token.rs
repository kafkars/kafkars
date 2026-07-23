//! Conservative identifier extraction from opaque macro token streams.

use std::collections::BTreeSet;

pub(crate) fn macro_identifiers(value: &syn::Macro) -> BTreeSet<String> {
    value
        .tokens
        .to_string()
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|token| {
            !token.is_empty()
                && token
                    .chars()
                    .next()
                    .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        })
        .map(str::to_owned)
        .collect()
}
