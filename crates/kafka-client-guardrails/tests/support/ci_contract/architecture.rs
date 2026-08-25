//! Exact provenance-free architecture entrypoint ordering.

pub(crate) fn violations(source: &str) -> Vec<String> {
    let actual = source
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect::<Vec<_>>();
    let expected = [
        "set -euo pipefail",
        "repo_root=$(cd \"$(dirname \"${BASH_SOURCE[0]}\")/..\" && pwd)",
        "cd \"$repo_root\"",
        "cargo test --locked -p kafka-client-guardrails --all-features",
        "git diff --check",
    ];
    if actual == expected {
        Vec::new()
    } else {
        vec![
            "check-architecture must remain provenance-free and contain only its reviewed commands"
                .to_owned(),
        ]
    }
}
