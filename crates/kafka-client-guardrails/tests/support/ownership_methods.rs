//! Method classification for protected-field mutation inspection.

use syn::Ident;

use super::MutationOwner;

const MUTATING_METHODS: &[&str] = &[
    "append",
    "capture",
    "clear",
    "clear_terminal",
    "complete",
    "drain",
    "entry",
    "extend",
    "get_mut",
    "insert",
    "lock",
    "pop",
    "pop_back",
    "pop_front",
    "push",
    "push_back",
    "push_front",
    "release",
    "remove",
    "reserve",
    "retain",
    "retain_committed_tail",
    "retain_generated",
    "retain_tail",
    "retain_terminal",
    "store",
    "take",
    "take_effects",
    "take_generated",
    "try_lock",
    "try_reserve",
];

pub(super) fn is_mutating_method(method: &Ident) -> bool {
    MUTATING_METHODS.iter().any(|candidate| method == candidate)
}

pub(super) fn is_non_owning_access(rule: &MutationOwner, method: &Ident) -> bool {
    matches!(method.to_string().as_str(), "as_ref" | "iter" | "len")
        || (method == "as_mut"
            && rule.owner_type == "EngineHostResources"
            && rule.field == "driver")
}
