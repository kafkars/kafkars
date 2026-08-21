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


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain one JSON object")
    return value


def required_scenarios(matrix: dict[str, Any], profile: str) -> list[str]:
    profiles = matrix.get("profiles", {})
    selected = profiles.get(profile)
    if not isinstance(selected, dict):
        raise ValueError(f"unknown qualification profile {profile!r}")
    source = selected.get("scenarios_from")
    if source is not None:
        return required_scenarios(matrix, str(source))
    scenarios = selected.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError(f"profile {profile!r} has no required scenarios")
    if len(scenarios) != len(set(scenarios)) or not all(isinstance(v, str) for v in scenarios):
        raise ValueError(f"profile {profile!r} scenarios must be unique strings")
    return scenarios


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


def build_cell(args: argparse.Namespace) -> dict[str, Any]:
    matrix = load_json(args.matrix)
    versions = matrix.get("kafka_versions", {})
    version_policy = versions.get(args.kafka_version)
    if not isinstance(version_policy, dict):
        raise ValueError(f"unknown Kafka version {args.kafka_version!r}")
    profile_policy = matrix["profiles"].get(args.profile)
    if not isinstance(profile_policy, dict):
        raise ValueError(f"unknown qualification profile {args.profile!r}")
    if args.security not in profile_policy.get("securities", []):
        raise ValueError(f"security {args.security!r} is outside profile {args.profile!r}")
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
    required = required_scenarios(matrix, args.profile)
    unexpected = sorted(set(observed).difference(required))
    if unexpected:
        raise ValueError(f"events contain unexpected scenarios: {', '.join(unexpected)}")
    scenarios = [
        observed.get(name, {"id": name, "status": "missing", "duration_ms": None})
        for name in required
    ]
    qualified = all(item["status"] == "passed" for item in scenarios)
    return {
        "profile": args.profile,
        "kafka_version": args.kafka_version,
        "lane": version_policy["lane"],
        "gating": bool(version_policy["gating"]),
        "maintained_upstream": bool(version_policy["maintained"]),
        "cluster_size": int(profile_policy["cluster_size"]),
        "security": args.security,
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
    profile = cell.get("profile")
    version = cell.get("kafka_version")
    security = cell.get("security")
    versions = matrix.get("kafka_versions", {})
    profiles = matrix.get("profiles", {})
    version_policy = versions.get(version)
    profile_policy = profiles.get(profile)
    if not isinstance(version_policy, dict) or not isinstance(profile_policy, dict):
        raise ValueError("stored cell names an unknown version or profile")
    if security not in profile_policy.get("securities", []):
        raise ValueError("stored cell names an unsupported security profile")
    exact = {
        "lane": version_policy["lane"],
        "gating": bool(version_policy["gating"]),
        "maintained_upstream": bool(version_policy["maintained"]),
        "cluster_size": int(profile_policy["cluster_size"]),
        "image": f"{matrix['image_repository']}:{version}",
    }
    if any(cell.get(name) != value for name, value in exact.items()):
        raise ValueError("stored cell conflicts with the qualification matrix")
    if DIGEST_RE.fullmatch(str(cell.get("image_digest", ""))) is None:
        raise ValueError("stored cell has no exact image digest")
    for name in ("client_sha", "driver_sha", "wire_sha"):
        if SHA_RE.fullmatch(str(cell.get(name, ""))) is None:
            raise ValueError(f"stored cell has no exact {name}")

    scenarios = cell.get("scenarios")
    if not isinstance(scenarios, list):
        raise ValueError("stored cell scenarios must be a list")
    required = required_scenarios(matrix, str(profile))
    if [item.get("id") for item in scenarios if isinstance(item, dict)] != required:
        raise ValueError("stored cell scenarios do not match the qualification profile")
    for item in scenarios:
        status = item.get("status")
        duration = item.get("duration_ms")
        if status not in {"passed", "failed", "missing"}:
            raise ValueError("stored cell contains an invalid scenario status")
        if duration is not None and (not isinstance(duration, int) or duration < 0):
            raise ValueError("stored cell contains an invalid scenario duration")
    qualified = all(item["status"] == "passed" for item in scenarios)
    duration_ms = sum(item["duration_ms"] or 0 for item in scenarios)
    if cell.get("qualified") is not qualified or cell.get("duration_ms") != duration_ms:
        raise ValueError("stored cell summary conflicts with its scenarios")


def markdown(evidence: dict[str, Any]) -> str:
    lines = [
        "# Compatibility qualification",
        "",
        f"Generated at `{evidence['generated_at']}` from exact archived evidence.",
        "",
        "| Kafka | Profile | Security | Lane | Qualified | Duration (ms) | Image digest |",
        "| --- | --- | --- | --- | --- | ---: | --- |",
    ]
    for cell in evidence["cells"]:
        lines.append(
            f"| {cell['kafka_version']} | {cell['profile']} | {cell['security']} | "
            f"{cell['lane']} | {'yes' if cell['qualified'] else 'no'} | {cell['duration_ms']} | "
            f"`{cell['image_digest']}` |"
        )
    for cell in evidence["cells"]:
        lines.extend(["", f"## Kafka {cell['kafka_version']} / {cell['security']}", ""])
        lines.extend(
            f"- `{item['id']}`: {item['status']}"
            + ("" if item["duration_ms"] is None else f" ({item['duration_ms']} ms)")
            for item in cell["scenarios"]
        )
    return "\n".join(lines) + "\n"


def evidence_document(cells: list[dict[str, Any]]) -> dict[str, Any]:
    keys = [(cell["profile"], cell["kafka_version"], cell["security"]) for cell in cells]
    if len(keys) != len(set(keys)):
        raise ValueError("qualification evidence contains duplicate cells")
    cells.sort(key=lambda cell: (cell["kafka_version"], cell["profile"], cell["security"]), reverse=True)
    return {
        "schema_version": 1,
        "generated_at": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
        "qualified": bool(cells) and all(cell["qualified"] for cell in cells),
        "cells": cells,
    }


def write_outputs(output: Path, evidence: dict[str, Any], support: Path | None) -> None:
    output.mkdir(parents=True, exist_ok=True)
    (output / "compatibility.json").write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    rendered = markdown(evidence)
    (output / "COMPATIBILITY.md").write_text(rendered, encoding="utf-8")
    if support is not None:
        source = support.read_text(encoding="utf-8")
        if source.count(BEGIN) != 1 or source.count(END) != 1:
            raise ValueError("SUPPORT source must contain one evidence marker pair")
        before, remainder = source.split(BEGIN)
        _, after = remainder.split(END)
        table = rendered.split("\n\n", 2)[2].split("\n\n", 1)[0]
        (output / "SUPPORT.md").write_text(
            f"{before}{BEGIN}\n{table}\n{END}{after}", encoding="utf-8"
        )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)
    cell = commands.add_parser("cell")
    cell.add_argument("--matrix", type=Path, required=True)
    cell.add_argument("--profile", required=True)
    cell.add_argument("--kafka-version", required=True)
    cell.add_argument("--security", required=True)
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
    merge.add_argument("--require-qualified", action="store_true")
    return root


def main() -> int:
    args = parser().parse_args()
    if args.command == "cell":
        evidence = evidence_document([build_cell(args)])
    else:
        matrix = load_json(args.matrix)
        cells = []
        for path in args.cell:
            document = load_json(path)
            source = document.get("cells")
            if not isinstance(source, list):
                raise ValueError(f"{path} does not contain qualification cells")
            for cell in source:
                if not isinstance(cell, dict):
                    raise ValueError(f"{path} contains a non-object qualification cell")
                validate_cell(matrix, cell)
            cells.extend(source)
        evidence = evidence_document(cells)
    write_outputs(args.output, evidence, args.support)
    return int(args.require_qualified and not evidence["qualified"])


if __name__ == "__main__":
    raise SystemExit(main())
