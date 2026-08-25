//! Rust evidence entrypoints contain only their reviewed local commands.

pub(crate) fn lint_violations(source: &str) -> Vec<String> {
    violations(
        source,
        &[
            "cargo fmt --all -- --check",
            "cargo clippy --locked --workspace --all-targets --all-features -- -D warnings",
            "RUSTDOCFLAGS=\"-D warnings\" cargo doc --locked --workspace --all-features --no-deps",
        ],
        "check-rust-lint",
    )
}

pub(crate) fn test_violations(source: &str) -> Vec<String> {
    violations(
        source,
        &["cargo test --locked --workspace --all-features"],
        "check-rust-test",
    )
}

fn violations(source: &str, commands: &[&str], label: &str) -> Vec<String> {
    let actual = source
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect::<Vec<_>>();
    let mut expected = vec![
        "set -euo pipefail",
        "repo_root=$(cd \"$(dirname \"${BASH_SOURCE[0]}\")/..\" && pwd)",
        "cd \"$repo_root\"",
    ];
    expected.extend_from_slice(commands);
    if actual == expected {
        Vec::new()
    } else {
        vec![format!(
            "{label} must remain provenance-free and contain only its reviewed commands"
        )]
    }
}
