//! Shared sibling-test discovery and declaration parsing.

use std::path::{Path, PathBuf};

use syn::punctuated::Punctuated;
use syn::{Attribute, Expr, Item, Lit, Meta, Token};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Declaration {
    Gated,
    Ungated,
    Redirected,
    Disabled,
    Absent,
}

pub(crate) fn is_unit_test(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.ends_with("_test.rs"))
        && path.components().any(|part| part.as_os_str() == "src")
}

pub(crate) fn sibling_facade(path: &Path) -> Option<PathBuf> {
    let parent = path.parent()?;
    let direct = ["mod.rs", "lib.rs", "main.rs"]
        .iter()
        .map(|name| parent.join(name))
        .find(|candidate| candidate.is_file());
    if direct.is_some() {
        return direct;
    }
    let module = parent.file_name()?.to_str()?;
    let split = parent.parent()?.join(format!("{module}.rs"));
    split.is_file().then_some(split)
}

pub(crate) fn declaration(source: &str, stem: &str, file_name: &str) -> Declaration {
    let Ok(syntax) = syn::parse_file(source) else {
        return Declaration::Absent;
    };
    let mut redirected = false;
    for item in syntax.items {
        let Item::Mod(module) = item else {
            continue;
        };
        if module.content.is_some() {
            continue;
        }
        let explicit_paths = module
            .attrs
            .iter()
            .filter_map(module_path)
            .collect::<Vec<_>>();
        let explicit_path = explicit_paths.first().map(String::as_str);
        let names_sibling = module.ident == stem && explicit_path.is_none();
        let points_to_sibling = explicit_path == Some(file_name);
        if module.ident == stem && explicit_path.is_some() && !points_to_sibling {
            redirected = true;
        }
        if !names_sibling && !points_to_sibling {
            continue;
        }
        if explicit_paths.len() != usize::from(explicit_path.is_some())
            || module.attrs.iter().any(cfg_attr_redirects_path)
        {
            return Declaration::Redirected;
        }
        let cfgs = module
            .attrs
            .iter()
            .filter(|attribute| attribute.path().is_ident("cfg"))
            .collect::<Vec<_>>();
        if !cfgs.iter().any(|attribute| is_cfg_test(attribute)) {
            return Declaration::Ungated;
        }
        if cfgs.len() != 1 || module.attrs.iter().any(cfg_attr_changes_compilation) {
            return Declaration::Disabled;
        }
        return Declaration::Gated;
    }
    if redirected {
        Declaration::Redirected
    } else {
        Declaration::Absent
    }
}

fn module_path(attribute: &Attribute) -> Option<String> {
    let Meta::NameValue(name_value) = &attribute.meta else {
        return None;
    };
    if !name_value.path.is_ident("path") {
        return None;
    }
    let Expr::Lit(expression) = &name_value.value else {
        return None;
    };
    let Lit::Str(value) = &expression.lit else {
        return None;
    };
    Some(value.value())
}

fn is_cfg_test(attribute: &Attribute) -> bool {
    attribute.path().is_ident("cfg")
        && attribute
            .parse_args::<syn::Path>()
            .is_ok_and(|path| path.is_ident("test"))
}

fn cfg_attr_redirects_path(attribute: &Attribute) -> bool {
    cfg_attr_entries(attribute).is_some_and(|entries| entries.iter().skip(1).any(meta_selects_path))
}

fn cfg_attr_changes_compilation(attribute: &Attribute) -> bool {
    cfg_attr_entries(attribute).is_some_and(|entries| {
        entries.iter().skip(1).any(|entry| match entry {
            Meta::Path(path) => path.is_ident("test"),
            Meta::List(list) => list.path.is_ident("cfg") || list.path.is_ident("cfg_attr"),
            Meta::NameValue(_) => false,
        })
    })
}

fn cfg_attr_entries(attribute: &Attribute) -> Option<Punctuated<Meta, Token![,]>> {
    attribute
        .path()
        .is_ident("cfg_attr")
        .then(|| attribute.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated))
        .transpose()
        .ok()
        .flatten()
}

fn meta_selects_path(meta: &Meta) -> bool {
    match meta {
        Meta::NameValue(value) => value.path.is_ident("path"),
        Meta::List(list) if list.path.is_ident("cfg_attr") => {
            match list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) {
                Ok(entries) => entries.iter().skip(1).any(meta_selects_path),
                Err(_) => true,
            }
        }
        Meta::Path(_) | Meta::List(_) => false,
    }
}
