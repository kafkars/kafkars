//! Shared sibling-test discovery and declaration parsing.

use std::path::{Path, PathBuf};

use syn::{Attribute, Expr, Item, Lit, Meta};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Declaration {
    Gated,
    Ungated,
    Redirected,
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
        let explicit_path = module.attrs.iter().find_map(module_path);
        let names_sibling = module.ident == stem && explicit_path.is_none();
        let points_to_sibling = explicit_path.as_deref() == Some(file_name);
        if module.ident == stem && explicit_path.is_some() && !points_to_sibling {
            redirected = true;
        }
        if !names_sibling && !points_to_sibling {
            continue;
        }
        return if module.attrs.iter().any(is_cfg_test) {
            Declaration::Gated
        } else {
            Declaration::Ungated
        };
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
