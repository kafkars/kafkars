//! Fail-closed detection of uninspectable Rust source expansion through macros.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use syn::visit::Visit;
use syn::{Attribute, Block, ItemForeignMod, ItemImpl, ItemMacro, ItemMod, ItemTrait, Macro};

use super::{MacroScope, display_path, source_capable_definition};

pub(crate) fn rust_source_expansion_violation(workspace: &Path, path: &Path) -> Option<String> {
    let source = fs::read_to_string(path).ok()?;
    let syntax = syn::parse_file(&source).ok()?;
    let mut collector = SourceExpansionCollector {
        scope: MacroScope::inspect(&syntax),
        local_macros: vec![BTreeSet::new()],
        ..SourceExpansionCollector::default()
    };
    collector.visit_file(&syntax);
    if collector.scope.has_opaque_macro_import() {
        return Some(format!(
            "{} uses opaque #[macro_use] import across source-file scopes",
            display_path(workspace, path)
        ));
    }
    if collector.include {
        return Some(format!(
            "{} uses include! for uninspected generated or external Rust",
            display_path(workspace, path)
        ));
    }
    collector.opaque_module_macro.then(|| {
        format!(
            "{} uses opaque macro expansion capable of emitting an external #[path] module",
            display_path(workspace, path)
        )
    })
}

#[derive(Default)]
struct SourceExpansionCollector {
    include: bool,
    opaque_module_macro: bool,
    scope: MacroScope,
    local_macros: Vec<BTreeSet<String>>,
}

impl<'ast> Visit<'ast> for SourceExpansionCollector {
    fn visit_item_macro(&mut self, item: &'ast ItemMacro) {
        let definition = item.mac.path.is_ident("macro_rules");
        if definition && source_capable_definition(&item.mac.tokens.to_string()) {
            self.opaque_module_macro = true;
        }
        syn::visit::visit_item_macro(self, item);
        if definition
            && !conditionally_compiled(&item.attrs)
            && let Some(name) = &item.ident
            && let Some(visible) = self.local_macros.last_mut()
        {
            visible.insert(name.to_string());
        }
    }

    fn visit_macro(&mut self, invocation: &'ast Macro) {
        let name = invocation
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string());
        let direct = name.as_deref() == Some("include");
        let tokens = invocation.tokens.to_string();
        let nested = tokens_name_identifier(&tokens, "include");
        if direct || nested {
            self.include = true;
        } else if name.as_deref() != Some("macro_rules") {
            let trusted_builtin = name
                .as_deref()
                .is_some_and(|name| self.scope.trusts(&invocation.path, name));
            let proven_local = name
                .as_deref()
                .is_some_and(|name| self.proves_visible_local(name));
            if !trusted_builtin && (!proven_local || source_capable_invocation(&tokens)) {
                // A bang macro can expand an item even when its invocation occurs
                // in a block or statement position. Unknown expansion is therefore
                // not a trustworthy source-graph boundary.
                self.opaque_module_macro = true;
            }
        }
        syn::visit::visit_macro(self, invocation);
    }

    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        if item.content.is_none() {
            syn::visit::visit_item_mod(self, item);
            return;
        }
        self.local_macros.push(BTreeSet::new());
        syn::visit::visit_item_mod(self, item);
        self.local_macros.pop();
    }

    fn visit_block(&mut self, block: &'ast Block) {
        self.local_macros.push(BTreeSet::new());
        syn::visit::visit_block(self, block);
        self.local_macros.pop();
    }

    fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
        self.local_macros.push(BTreeSet::new());
        syn::visit::visit_item_impl(self, item);
        self.local_macros.pop();
    }

    fn visit_item_trait(&mut self, item: &'ast ItemTrait) {
        self.local_macros.push(BTreeSet::new());
        syn::visit::visit_item_trait(self, item);
        self.local_macros.pop();
    }

    fn visit_item_foreign_mod(&mut self, item: &'ast ItemForeignMod) {
        self.local_macros.push(BTreeSet::new());
        syn::visit::visit_item_foreign_mod(self, item);
        self.local_macros.pop();
    }
}

impl SourceExpansionCollector {
    fn proves_visible_local(&self, name: &str) -> bool {
        self.scope.permits_local(name)
            && self
                .local_macros
                .iter()
                .rev()
                .any(|visible| visible.contains(name))
    }
}

fn conditionally_compiled(attributes: &[Attribute]) -> bool {
    attributes
        .iter()
        .any(|attribute| attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr"))
}

fn source_capable_invocation(tokens: &str) -> bool {
    ["mod", "path", "!"]
        .iter()
        .any(|candidate| tokens_name_identifier(tokens, candidate))
}

fn tokens_name_identifier(tokens: &str, candidate: &str) -> bool {
    tokens.split_whitespace().any(|token| token == candidate)
}
