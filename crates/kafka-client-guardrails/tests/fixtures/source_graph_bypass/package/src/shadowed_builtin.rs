//! Qualified macros cannot inherit trust from a shadowed builtin root.

mod evil {
    pub use ::std::include as format;
}

use crate::evil as std;

std::format!("hidden.inc");
