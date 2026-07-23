//! Runnable-test evidence discovery for registered sibling mirrors.

use syn::{Attribute, Item, ItemFn};

pub(crate) fn runnable_test_count(source: &str) -> Result<usize, syn::Error> {
    let syntax = syn::parse_file(source)?;
    if syntax.attrs.iter().any(disables_default_test) {
        return Ok(0);
    }
    Ok(runnable_items(&syntax.items))
}

fn runnable_items(items: &[Item]) -> usize {
    items
        .iter()
        .map(|item| match item {
            Item::Fn(function) => usize::from(runnable_test(function)),
            Item::Mod(module) if !module.attrs.iter().any(disables_default_test) => module
                .content
                .as_ref()
                .map_or(0, |(_, items)| runnable_items(items)),
            _ => 0,
        })
        .sum()
}

fn runnable_test(function: &ItemFn) -> bool {
    function
        .attrs
        .iter()
        .any(|attribute| attribute.path().is_ident("test"))
        && !function
            .attrs
            .iter()
            .any(|attribute| attribute.path().is_ident("ignore"))
        && !function.attrs.iter().any(disables_default_test)
}

fn disables_default_test(attribute: &Attribute) -> bool {
    if attribute.path().is_ident("cfg_attr") {
        return true;
    }
    attribute.path().is_ident("cfg")
        && !attribute
            .parse_args::<syn::Path>()
            .is_ok_and(|path| path.is_ident("test"))
}
