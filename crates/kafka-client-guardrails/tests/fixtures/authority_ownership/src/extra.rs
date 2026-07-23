//! Authority declaration with an unregistered private field.

pub(crate) struct ExtraAuthority {
    expected: usize,
    unregistered: usize,
}
