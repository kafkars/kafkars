//! External macro source deliberately outside guardrail workspace ownership.

#[macro_export]
macro_rules! opaque {
    () => {
        #[path = "hidden.rs"]
        mod hidden;
    };
}
