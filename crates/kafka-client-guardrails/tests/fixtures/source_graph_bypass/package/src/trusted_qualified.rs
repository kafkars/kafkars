//! Genuine qualified macro roots remain accepted by source inspection.

pub fn qualified_builtins_are_visible() {
    std::assert!(true);
    core::assert!(true);
    let _: syn::Token![,];
}
