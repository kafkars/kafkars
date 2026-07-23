//! Authority declaration in a module that owns a child module.

mod child;

pub(crate) struct NonLeafAuthority {
    private_field: usize,
}
