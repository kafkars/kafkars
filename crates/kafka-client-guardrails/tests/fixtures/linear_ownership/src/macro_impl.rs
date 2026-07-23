//! Opaque macro tokens cannot generate duplication implementations.

macro_rules! duplicate {
    ($($tokens:tt)*) => {};
}

duplicate! {
    impl Clone for HiddenOwner {}
}
