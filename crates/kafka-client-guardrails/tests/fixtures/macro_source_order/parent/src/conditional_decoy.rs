//! A disabled safe definition cannot authorize an inherited invocation.

#[cfg(any())]
macro_rules! opaque {
    () => {};
}

opaque!();
