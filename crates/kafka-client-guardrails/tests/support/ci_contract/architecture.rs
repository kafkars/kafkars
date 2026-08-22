//! Exact architecture entrypoint ordering for live and synthetic provenance.

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
        "\"$repo_root/scripts/check-dependency-provenance\"",
        "\"$repo_root/scripts/test-dependency-provenance\"",
        "\"$repo_root/scripts/test-bootstrap-siblings\"",
        "PYTHONDONTWRITEBYTECODE=1 python3 -m unittest qualification.test_render -v",
        "cargo test --locked -p kafka-client-guardrails --all-features",
        "git diff --check",
    ];
    if actual == expected {
        Vec::new()
    } else {
        vec![
            "check-architecture must run exact and synthetic provenance before guardrails"
                .to_owned(),
        ]
    }
}
