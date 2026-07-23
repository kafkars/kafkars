//! An inaccessible safe macro cannot launder an imported opaque macro.

mod decoy {
    macro_rules! format {
        () => {};
    }
}

mod evil {
    pub use ::std::include as format;
}

use self::evil::format;

format!("hidden.inc");
