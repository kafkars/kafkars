//! Macro-generated external module declaration that must fail closed.

macro_rules! external_module {
    () => {
        #[path = "../escaped.rs"]
        mod hidden;
    };
}

external_module!();
