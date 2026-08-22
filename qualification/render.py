#!/usr/bin/env python3
"""Validate qualification events and render machine and human evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
BEGIN = "<!-- qualification-evidence:begin -->"
END = "<!-- qualification-evidence:end -->"
RUNNER_STATUSES = {"passed", "failed"}
LIBTEST_NAME_RE = re.compile(r"^[A-Za-z0-9_:]+$")


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain one JSON object")
    return value


def string_list(value: Any, label: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, str) and item for item in value)
        or len(value) != len(set(value))
    ):
        raise ValueError(f"{label} must be unique nonempty strings")
    return value


def validate_matrix(matrix: dict[str, Any]) -> None:
    if matrix.get("schema_version") != 2:
        raise ValueError("qualification matrix schema_version must be 2")
    repository = matrix.get("image_repository")
    if not isinstance(repository, str) or not repository:
        raise ValueError("qualification matrix must name one image repository")

    versions = matrix.get("kafka_versions")
    if not isinstance(versions, dict) or not versions:
        raise ValueError("qualification matrix must name Kafka versions")
    for version, policy in versions.items():
        if not isinstance(version, str) or not version or not isinstance(policy, dict):
            raise ValueError("Kafka version policies must be named objects")
        if not isinstance(policy.get("lane"), str) or not policy["lane"]:
            raise ValueError(f"Kafka {version} must name one lane")
        if type(policy.get("maintained")) is not bool:
            raise ValueError(f"Kafka {version} maintained must be boolean")

    profiles = matrix.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        raise ValueError("qualification matrix must name profiles")
    for profile, policy in profiles.items():
        if not isinstance(profile, str) or not profile or not isinstance(policy, dict):
            raise ValueError("qualification profiles must be named objects")
        cluster_size = policy.get("cluster_size")
        if type(cluster_size) is not int or cluster_size <= 0:
            raise ValueError(f"profile {profile!r} cluster size must be a positive integer")
        securities = string_list(policy.get("securities"), f"profile {profile!r} securities")
        base = string_list(policy.get("scenarios"), f"profile {profile!r} scenarios")
        additions = policy.get("security_scenarios", {})
        if not isinstance(additions, dict):
            raise ValueError(f"profile {profile!r} security scenarios must be an object")
        for security, scenarios in additions.items():
            if security not in securities:
                raise ValueError(
                    f"profile {profile!r} names scenarios for unsupported security {security!r}"
                )
            extra = string_list(
                scenarios, f"profile {profile!r} security {security!r} scenarios"
            )
            if set(base).intersection(extra):
                raise ValueError(
                    f"profile {profile!r} security scenarios duplicate base scenarios"
                )

    evidence_sets = matrix.get("evidence_sets")
    if not isinstance(evidence_sets, dict) or not evidence_sets:
        raise ValueError("qualification matrix must name evidence sets")
    required_keys = {"profile", "kafka_version", "security", "gating"}
    for evidence_set, cells in evidence_sets.items():
        if (
            not isinstance(evidence_set, str)
            or not evidence_set
            or not isinstance(cells, list)
            or not cells
        ):
            raise ValueError("qualification evidence sets must be named nonempty lists")
        keys = []
        for cell in cells:
            if not isinstance(cell, dict) or set(cell) != required_keys:
                raise ValueError(
                    f"evidence set {evidence_set!r} cells must contain exactly {sorted(required_keys)!r}"
                )
            profile = cell["profile"]
            version = cell["kafka_version"]
            security = cell["security"]
            if profile not in profiles or version not in versions:
                raise ValueError(f"evidence set {evidence_set!r} names unknown policy")
            if security not in profiles[profile]["securities"]:
                raise ValueError(
                    f"evidence set {evidence_set!r} names unsupported security {security!r}"
                )
            if type(cell["gating"]) is not bool:
                raise ValueError(f"evidence set {evidence_set!r} gating must be boolean")
            keys.append((profile, version, security))
        if len(keys) != len(set(keys)):
            raise ValueError(f"evidence set {evidence_set!r} contains duplicate cells")


def required_scenarios(
    matrix: dict[str, Any], profile: str, security: str | None = None
) -> list[str]:
    selected = matrix.get("profiles", {}).get(profile)
    if not isinstance(selected, dict):
        raise ValueError(f"unknown qualification profile {profile!r}")
    scenarios = list(
        string_list(selected.get("scenarios"), f"profile {profile!r} scenarios")
    )
    if security is None:
        return scenarios
    if security not in selected.get("securities", []):
        raise ValueError(f"security {security!r} is outside profile {profile!r}")
    additions = selected.get("security_scenarios", {}).get(security, [])
    scenarios.extend(additions)
    if len(scenarios) != len(set(scenarios)):
        raise ValueError(f"profile {profile!r} has duplicate effective scenarios")
    return scenarios


def expected_cell(
    matrix: dict[str, Any],
    evidence_set: str,
    profile: str,
    kafka_version: str,
    security: str,
) -> dict[str, Any]:
    cells = matrix.get("evidence_sets", {}).get(evidence_set)
    if not isinstance(cells, list):
        raise ValueError(f"unknown qualification evidence set {evidence_set!r}")
    matches = [
        cell
        for cell in cells
        if (
            cell["profile"] == profile
            and cell["kafka_version"] == kafka_version
            and cell["security"] == security
        )
    ]
    if len(matches) != 1:
        raise ValueError(
            f"cell {profile}/{kafka_version}/{security} is outside evidence set {evidence_set!r}"
        )
    return matches[0]


def parse_events(path: Path) -> dict[str, dict[str, Any]]:
    events: dict[str, dict[str, Any]] = {}
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        fields = raw.split("\t")
        if len(fields) != 3 or fields[1] not in {"passed", "failed"}:
            raise ValueError(f"{path}:{number} is not scenario/status/duration TSV")
        scenario, status, duration_text = fields
        if scenario in events:
            raise ValueError(f"{path}:{number} duplicates scenario {scenario!r}")
        duration_ms = int(duration_text)
        if duration_ms < 0:
            raise ValueError(f"{path}:{number} has a negative duration")
        events[scenario] = {"id": scenario, "status": status, "duration_ms": duration_ms}
    return events


def validate_libtest_listing(path: Path, test_name: str) -> None:
    if LIBTEST_NAME_RE.fullmatch(test_name) is None:
        raise ValueError("libtest name must be one exact Rust test path")
    expected = f"{test_name}: test"
    matches = sum(
        line == expected
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines()
    )
    if matches != 1:
        raise ValueError(
            f"libtest listing must contain exactly one {expected!r}; found {matches}"
        )


def build_cell(args: argparse.Namespace) -> dict[str, Any]:
    matrix = load_json(args.matrix)
    validate_matrix(matrix)
    policy = expected_cell(
        matrix, args.evidence_set, args.profile, args.kafka_version, args.security
    )
    versions = matrix["kafka_versions"]
    profiles = matrix["profiles"]
    version_policy = versions[args.kafka_version]
    profile_policy = profiles[args.profile]
    if args.runner_status not in RUNNER_STATUSES:
        raise ValueError("runner status must be passed or failed")
    for name, value in {
        "client SHA": args.client_sha,
        "driver SHA": args.driver_sha,
        "wire SHA": args.wire_sha,
    }.items():
        if SHA_RE.fullmatch(value) is None:
            raise ValueError(f"{name} must be one full lowercase commit SHA")
    if DIGEST_RE.fullmatch(args.image_digest) is None:
        raise ValueError("image digest must be one exact sha256 digest")
    expected_image = f"{matrix['image_repository']}:{args.kafka_version}"
    if args.image != expected_image:
        raise ValueError(f"image must be {expected_image!r}")

    observed = parse_events(args.events)
    required = required_scenarios(matrix, args.profile, args.security)
    unexpected = sorted(set(observed).difference(required))
    if unexpected:
        raise ValueError(f"events contain unexpected scenarios: {', '.join(unexpected)}")
    scenarios = [
        observed.get(name, {"id": name, "status": "missing", "duration_ms": None})
        for name in required
    ]
    qualified = args.runner_status == "passed" and all(
        item["status"] == "passed" for item in scenarios
    )
    return {
        "evidence_set": args.evidence_set,
        "profile": args.profile,
        "kafka_version": args.kafka_version,
        "lane": version_policy["lane"],
        "gating": policy["gating"],
        "maintained_upstream": version_policy["maintained"],
        "cluster_size": profile_policy["cluster_size"],
        "security": args.security,
        "runner_status": args.runner_status,
        "qualified": qualified,
        "image": args.image,
        "image_digest": args.image_digest,
        "client_sha": args.client_sha,
        "driver_sha": args.driver_sha,
        "wire_sha": args.wire_sha,
        "duration_ms": sum(item["duration_ms"] or 0 for item in scenarios),
        "scenarios": scenarios,
    }


def validate_cell(matrix: dict[str, Any], cell: dict[str, Any]) -> None:
    validate_matrix(matrix)
    evidence_set = cell.get("evidence_set")
    profile = cell.get("profile")
    version = cell.get("kafka_version")
    security = cell.get("security")
    if not all(
        isinstance(value, str)
        for value in (evidence_set, profile, version, security)
    ):
        raise ValueError("stored cell has invalid policy names")
    policy = expected_cell(matrix, evidence_set, profile, version, security)
    version_policy = matrix["kafka_versions"][version]
    profile_policy = matrix["profiles"][profile]
    exact = {
        "lane": version_policy["lane"],
        "gating": policy["gating"],
        "maintained_upstream": version_policy["maintained"],
        "cluster_size": profile_policy["cluster_size"],
        "image": f"{matrix['image_repository']}:{version}",
    }
    if any(
        type(cell.get(name)) is not type(value) or cell.get(name) != value
        for name, value in exact.items()
    ):
        raise ValueError("stored cell conflicts with the qualification matrix")
    if cell.get("runner_status") not in RUNNER_STATUSES:
        raise ValueError("stored cell has invalid runner status")
    if DIGEST_RE.fullmatch(str(cell.get("image_digest", ""))) is None:
        raise ValueError("stored cell has no exact image digest")
    for name in ("client_sha", "driver_sha", "wire_sha"):
        if SHA_RE.fullmatch(str(cell.get(name, ""))) is None:
            raise ValueError(f"stored cell has no exact {name}")

    scenarios = cell.get("scenarios")
    if not isinstance(scenarios, list):
        raise ValueError("stored cell scenarios must be a list")
    required = required_scenarios(matrix, profile, security)
    if [item.get("id") for item in scenarios if isinstance(item, dict)] != required:
        raise ValueError("stored cell scenarios do not match the qualification profile")
    for item in scenarios:
        status = item.get("status")
        duration = item.get("duration_ms")
        if status not in {"passed", "failed", "missing"}:
            raise ValueError("stored cell contains an invalid scenario status")
        if duration is not None and (type(duration) is not int or duration < 0):
            raise ValueError("stored cell contains an invalid scenario duration")
    qualified = cell["runner_status"] == "passed" and all(
        item["status"] == "passed" for item in scenarios
    )
    duration_ms = sum(item["duration_ms"] or 0 for item in scenarios)
    if type(cell.get("duration_ms")) is not int or cell["duration_ms"] < 0:
        raise ValueError("stored cell has an invalid total duration")
    if cell.get("qualified") is not qualified or cell.get("duration_ms") != duration_ms:
        raise ValueError("stored cell summary conflicts with its scenarios or runner status")


def markdown(evidence: dict[str, Any]) -> str:
    lines = [
        "# Compatibility qualification",
        "",
        f"Generated at `{evidence['generated_at']}` from exact archived evidence.",
        "",
        f"- Evidence set: `{evidence['evidence_set']}`",
        f"- Client: `{evidence['client_sha']}`",
        f"- Driver: `{evidence['driver_sha']}`",
        f"- Wire: `{evidence['wire_sha']}`",
        "",
        (
            "| Kafka | Profile | Security | Lane | Runner | Qualified | "
            "Duration (ms) | Image digest |"
        ),
        "| --- | --- | --- | --- | --- | --- | ---: | --- |",
    ]
    for cell in evidence["cells"]:
        lines.append(
            f"| {cell['kafka_version']} | {cell['profile']} | {cell['security']} | "
            f"{cell['lane']} | {cell['runner_status']} | "
            f"{'yes' if cell['qualified'] else 'no'} | {cell['duration_ms']} | "
            f"`{cell['image_digest']}` |"
        )
    for cell in evidence["cells"]:
        lines.extend(
            [
                "",
                (
                    f"## Kafka {cell['kafka_version']} / {cell['profile']} / "
                    f"{cell['security']}"
                ),
                "",
            ]
        )
        lines.extend(
            f"- `{item['id']}`: {item['status']}"
            + ("" if item["duration_ms"] is None else f" ({item['duration_ms']} ms)")
            for item in cell["scenarios"]
        )
    return "\n".join(lines) + "\n"


def evidence_document(cells: list[dict[str, Any]]) -> dict[str, Any]:
    keys = [
        (
            cell["evidence_set"],
            cell["profile"],
            cell["kafka_version"],
            cell["security"],
        )
        for cell in cells
    ]
    if len(keys) != len(set(keys)):
        raise ValueError("qualification evidence contains duplicate cells")
    revisions = {
        (cell["client_sha"], cell["driver_sha"], cell["wire_sha"])
        for cell in cells
    }
    if len(revisions) > 1:
        raise ValueError("qualification cells do not use one exact crate graph")
    image_digests: dict[str, set[str]] = {}
    for cell in cells:
        image_digests.setdefault(cell["kafka_version"], set()).add(
            cell["image_digest"]
        )
    inconsistent_images = sorted(
        version for version, digests in image_digests.items() if len(digests) > 1
    )
    if inconsistent_images:
        raise ValueError(
            "qualification cells do not use one exact image digest per Kafka version: "
            + ", ".join(inconsistent_images)
        )
    evidence_sets = {cell["evidence_set"] for cell in cells}
    if len(evidence_sets) > 1:
        raise ValueError("qualification cells do not use one evidence set")
    client_sha, driver_sha, wire_sha = next(iter(revisions), (None, None, None))
    evidence_set = next(iter(evidence_sets), None)
    gating_cells = [cell for cell in cells if cell["gating"]]
    qualified_cells = gating_cells or cells
    cells.sort(
        key=lambda cell: (cell["kafka_version"], cell["profile"], cell["security"]),
        reverse=True,
    )
    return {
        "schema_version": 2,
        "generated_at": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
        "evidence_set": evidence_set,
        "qualified": bool(cells)
        and all(cell["qualified"] for cell in qualified_cells),
        "client_sha": client_sha,
        "driver_sha": driver_sha,
        "wire_sha": wire_sha,
        "cells": cells,
    }


def validate_evidence_document(
    matrix: dict[str, Any], document: dict[str, Any], path: Path
) -> list[dict[str, Any]]:
    if document.get("schema_version") != 2:
        raise ValueError(f"{path} is not qualification evidence schema 2")
    source = document.get("cells")
    if not isinstance(source, list) or not source:
        raise ValueError(f"{path} does not contain qualification cells")
    for cell in source:
        if not isinstance(cell, dict):
            raise ValueError(f"{path} contains a non-object qualification cell")
        validate_cell(matrix, cell)
    recomputed = evidence_document(list(source))
    if type(document.get("qualified")) is not bool:
        raise ValueError(f"{path} has a non-boolean qualification summary")
    summary_fields = (
        "evidence_set",
        "qualified",
        "client_sha",
        "driver_sha",
        "wire_sha",
    )
    if any(document.get(field) != recomputed[field] for field in summary_fields):
        raise ValueError(f"{path} summary conflicts with its qualification cells")
    return source


def require_complete_set(
    matrix: dict[str, Any], evidence: dict[str, Any], evidence_set: str
) -> None:
    policy = matrix.get("evidence_sets", {}).get(evidence_set)
    if not isinstance(policy, list):
        raise ValueError(f"unknown complete-set requirement {evidence_set!r}")
    expected = {
        (cell["profile"], cell["kafka_version"], cell["security"]) for cell in policy
    }
    actual = {
        (cell["profile"], cell["kafka_version"], cell["security"])
        for cell in evidence["cells"]
        if cell["evidence_set"] == evidence_set
    }
    wrong_set = [
        cell for cell in evidence["cells"] if cell["evidence_set"] != evidence_set
    ]
    missing = sorted(expected.difference(actual))
    unexpected = sorted(actual.difference(expected))
    if wrong_set or missing or unexpected:
        raise ValueError(
            f"{evidence_set} evidence is incomplete: "
            f"wrong_set={len(wrong_set)} missing={missing!r} unexpected={unexpected!r}"
        )


def write_outputs(output: Path, evidence: dict[str, Any], support: Path | None) -> None:
    output.mkdir(parents=True, exist_ok=True)
    rendered = markdown(evidence)
    support_projection = None
    if support is not None:
        source = support.read_text(encoding="utf-8")
        if source.count(BEGIN) != 1 or source.count(END) != 1:
            raise ValueError("SUPPORT source must contain one evidence marker pair")
        before, remainder = source.split(BEGIN)
        _, after = remainder.split(END)
        table_start = rendered.index("| Kafka |")
        table_end = rendered.index("\n\n", table_start)
        table = rendered[table_start:table_end]
        support_projection = f"{before}{BEGIN}\n{table}\n{END}{after}"
    (output / "compatibility.json").write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (output / "COMPATIBILITY.md").write_text(rendered, encoding="utf-8")
    if support_projection is not None:
        (output / "SUPPORT.md").write_text(support_projection, encoding="utf-8")


def add_policy_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--matrix", type=Path, required=True)
    command.add_argument("--evidence-set", required=True)
    command.add_argument("--profile", required=True)
    command.add_argument("--kafka-version", required=True)
    command.add_argument("--security", required=True)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)
    policy = commands.add_parser("policy")
    add_policy_arguments(policy)
    libtest = commands.add_parser("libtest")
    libtest.add_argument("--listing", type=Path, required=True)
    libtest.add_argument("--test", required=True)
    cell = commands.add_parser("cell")
    add_policy_arguments(cell)
    cell.add_argument("--runner-status", required=True)
    cell.add_argument("--image", required=True)
    cell.add_argument("--image-digest", required=True)
    cell.add_argument("--client-sha", required=True)
    cell.add_argument("--driver-sha", required=True)
    cell.add_argument("--wire-sha", required=True)
    cell.add_argument("--events", type=Path, required=True)
    cell.add_argument("--output", type=Path, required=True)
    cell.add_argument("--support", type=Path)
    cell.add_argument("--require-qualified", action="store_true")
    merge = commands.add_parser("merge")
    merge.add_argument("--matrix", type=Path, required=True)
    merge.add_argument("--cell", type=Path, action="append", required=True)
    merge.add_argument("--output", type=Path, required=True)
    merge.add_argument("--support", type=Path)
    merge.add_argument("--require-complete-set")
    merge.add_argument("--require-qualified", action="store_true")
    return root


def main() -> int:
    args = parser().parse_args()
    if args.command == "policy":
        matrix = load_json(args.matrix)
        validate_matrix(matrix)
        expected_cell(
            matrix, args.evidence_set, args.profile, args.kafka_version, args.security
        )
        return 0
    if args.command == "libtest":
        validate_libtest_listing(args.listing, args.test)
        return 0
    if args.command == "cell":
        evidence = evidence_document([build_cell(args)])
    else:
        matrix = load_json(args.matrix)
        validate_matrix(matrix)
        cells = []
        for path in args.cell:
            document = load_json(path)
            cells.extend(validate_evidence_document(matrix, document, path))
        evidence = evidence_document(cells)
        if args.require_complete_set is not None:
            require_complete_set(matrix, evidence, args.require_complete_set)
    write_outputs(args.output, evidence, args.support)
    return int(args.require_qualified and not evidence["qualified"])


if __name__ == "__main__":
    raise SystemExit(main())
