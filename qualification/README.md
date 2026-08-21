# Qualification evidence contract

`matrix.json` is the checked-in qualification policy. It names every Kafka
version lane, cluster size, security profile, and scenario that a run must
complete. A runner may not silently omit a scenario and still qualify.

Each scenario appends one tab-separated event with its stable ID, terminal
status, and elapsed milliseconds. `scripts/render-qualification cell` combines
those events with the exact Kafka image digest and the full client, driver, and
wire commit SHAs. Missing scenarios remain visible and make the cell fail
closed.

`scripts/render-qualification merge` revalidates every stored cell against the
checked-in matrix before producing `compatibility.json`, `COMPATIBILITY.md`,
and the evidence-owned table in `SUPPORT.md`. Broker logs and client diagnostics
remain separate archived files beside these summaries; the summaries never
stand in for the raw run evidence.

Run the pull-request cell from a clean checkout with its reviewed sibling pair:

```console
scripts/run-qualification pr-smoke 4.3.1 plaintext /tmp/kafkars-pr-evidence
```

The runner resolves the pulled tag to a repository digest before starting the
cluster, runs Compose with that immutable digest, and always captures broker
state and logs before removing its isolated cluster and volumes.

The scheduled matrix uses three combined KRaft broker/controllers and runs all
five client transport profiles for each policy version:

```console
scripts/run-qualification nightly 4.3.1 plaintext /tmp/kafkars-nightly-cell
scripts/run-qualification nightly 4.3.1 tls /tmp/kafkars-nightly-tls-cell
scripts/run-qualification nightly 4.3.1 sasl_plain /tmp/kafkars-nightly-plain-cell
scripts/run-qualification nightly 4.3.1 scram_sha_256 /tmp/kafkars-nightly-scram-cell
scripts/run-qualification nightly 4.3.1 scram_sha_512 /tmp/kafkars-nightly-scram512-cell
```

TLS certificates and stores are generated for each run and removed after log
capture. SASL lanes use public, qualification-only credentials against the
ephemeral cluster. Internal broker and controller traffic stays on an isolated
plaintext Compose network so each cell isolates the client-facing transport
contract under test.

The manual `release` workflow profile runs the same full matrix against one
exact proposed client, driver, and wire graph. It additionally archives the
packaged crates, package checksums, crate metadata, image inspection, broker
logs, client diagnostics, raw scenario timings, and aggregate compatibility
documents. Kafka 3.9.2 evidence is required to be present but remains a
non-gating legacy result.
