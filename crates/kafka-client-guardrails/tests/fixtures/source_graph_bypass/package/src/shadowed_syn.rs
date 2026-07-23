//! Local modules cannot impersonate the trusted `syn::Token` macro root.

mod syn {
    pub use ::std::include as Token;
}

syn::Token!("hidden.inc");
