#!/usr/bin/env python3
"""Gera o sidecar estático do Faria v2 a partir de citations.sqlite.

Somente resoluções exatas que possuam ao menos uma atestação lexical aceita são
publicadas. O arquivo preserva os IDs de entrada usados pelos shards do site.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from typing import Any
from urllib.parse import quote


FORMAT = "faria-v2-public-citations-2"
REPORT_FORMAT = "faria-v2-public-citations-report-2"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def open_read_only(path: Path) -> sqlite3.Connection:
    absolute = path.resolve()
    connection = sqlite3.connect(
        f"file:{quote(str(absolute))}?mode=ro&immutable=1", uri=True
    )
    connection.row_factory = sqlite3.Row
    return connection


def site_entry_ids(database: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    ordinals: Counter[str] = Counter()
    with open_read_only(database) as connection:
        for row in connection.execute("SELECT id FROM entry ORDER BY entry_id"):
            legacy_id = str(row["id"])
            document = legacy_id.split(":", 1)[0]
            ordinals[document] += 1
            result[legacy_id] = f"{document}-e{ordinals[document]}"
    return result


def atomic_json(path: Path, value: Any, force: bool) -> None:
    if path.exists() and not force:
        raise FileExistsError(f"output exists; use --force: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with open(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        temporary.chmod(0o644)
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def citation(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "claim_key": row["claim_key"],
        "source_citation_key": row["source_citation_key"],
        "reference": json.loads(row["raw_payload_json"])["faria_reference_raw"],
        "resolved_reference": row["reference_raw"],
        "author": {"key": row["author_key"], "name": row["author_name"]},
        "work": {"key": row["work_key"], "title": row["work_title"]},
        "passage": {
            "key": row["passage_key"],
            "locator_key": row["locator_key"],
            "locator": json.loads(row["locator_json"]),
            "text": row["display_text"],
            "text_sha256": row["display_text_sha256"],
            "url": row["document_url"],
        },
        "attestations": json.loads(row["attestations_json"]),
        "status": "exact",
        "confirmation": "morphological_analyzer",
    }


def export(
    v2_database: Path,
    citations_database: Path,
    output: Path,
    report_path: Path,
    force: bool,
    lookup_path: Path | None = None,
) -> dict[str, Any]:
    entry_ids = site_entry_ids(v2_database)
    entries: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with open_read_only(citations_database) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(citation_claim)")}
        if "raw_payload_json" not in columns:
            raise ValueError("citations database lacks citation_claim.raw_payload_json")
        rows = connection.execute(
            "SELECT p.*,c.raw_payload_json FROM published_citation p "
            "JOIN citation_claim c ON c.claim_key=p.claim_key "
            "WHERE p.outcome_status='exact' AND json_array_length(p.attestations_json)>0"
        ).fetchall()
        for row in rows:
            site_id = entry_ids.get(str(row["source_entry_key"]))
            if site_id is None:
                raise ValueError(f"citation references unknown v2 entry {row['source_entry_key']}")
            entries[site_id].append(citation(row))
    for records in entries.values():
        records.sort(key=lambda item: (item["reference"], item["claim_key"]))

    artifact: dict[str, Any] = {
        "format": FORMAT,
        "entries": dict(sorted(entries.items())),
        "provenance": {
            "v2_database_sha256": sha256_file(v2_database),
            "citations_database_sha256": sha256_file(citations_database),
            "publication_policy": "exact_resolution_with_accepted_lexical_attestation",
            "lexical_analyzer": "WordsWASM v0.2 search-only WWDB",
            "publishes_v3_lexical_content": False,
        },
    }
    shard_metrics: dict[str, Any] | None = None
    if lookup_path is not None:
        lookup = json.loads(lookup_path.read_text(encoding="utf-8"))
        if not isinstance(lookup, dict):
            raise ValueError("lookup must contain an object")
        by_block: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(dict)
        for site_id, records in artifact["entries"].items():
            position = lookup.get(site_id)
            if not isinstance(position, dict) or not position.get("block_file"):
                raise ValueError(f"lookup has no block for cited entry {site_id}")
            block_name = Path(str(position["block_file"])).name
            by_block[block_name][site_id] = records
        shard_dir = output.parent / "citations"
        shard_sizes: dict[str, int] = {}
        for block_name, block_entries in sorted(by_block.items()):
            shard_path = shard_dir / block_name
            atomic_json(
                shard_path,
                {
                    "format": FORMAT,
                    "block": block_name,
                    "entries": dict(sorted(block_entries.items())),
                },
                force,
            )
            shard_sizes[block_name] = shard_path.stat().st_size
        artifact["entries"] = {}
        artifact["sharded"] = True
        artifact["shard_path_template"] = "citations/{block_file}"
        artifact["shards"] = sorted(by_block)
        shard_metrics = {
            "count": len(by_block),
            "total_bytes": sum(shard_sizes.values()),
            "maximum_bytes": max(shard_sizes.values(), default=0),
        }
    atomic_json(output, artifact, force)
    report = {
        "format": REPORT_FORMAT,
        "status": "validated",
        "output": str(output.resolve()),
        "output_sha256": sha256_file(output),
        "v2_database": str(v2_database.resolve()),
        "v2_database_sha256": artifact["provenance"]["v2_database_sha256"],
        "citations_database": str(citations_database.resolve()),
        "citations_database_sha256": artifact["provenance"]["citations_database_sha256"],
        "matched_citations": sum(len(values) for values in entries.values()),
        "matched_v2_entries": len(entries),
        "shards": shard_metrics,
        "contracts": {
            "source_databases_read_only_immutable": True,
            "exact_resolutions_only": True,
            "accepted_lexical_attestation_required": True,
            "publishes_v3_definitions_or_notes": False,
            "build_is_atomic": True,
        },
    }
    atomic_json(report_path, report, force)
    return report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--v2-db", type=Path, required=True)
    result.add_argument("--citations-db", type=Path, required=True)
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--report", type=Path, required=True)
    result.add_argument("--lookup", type=Path)
    result.add_argument("--force", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        report = export(
            args.v2_db,
            args.citations_db,
            args.output,
            args.report,
            args.force,
            args.lookup,
        )
    except (FileNotFoundError, FileExistsError, ValueError, sqlite3.Error, json.JSONDecodeError) as error:
        print(f"erro: {error}")
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
