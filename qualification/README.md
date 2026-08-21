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
