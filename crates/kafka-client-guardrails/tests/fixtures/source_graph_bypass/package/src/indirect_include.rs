//! Identifier-mediated include expansion that must fail closed.

macro_rules! load {
    ($loader:ident) => {
        $loader!(concat!(env!("OUT_DIR"), "/indirect.rs"));
    };
}

load!(include);
