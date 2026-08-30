#!/usr/bin/env python3
"""Exporta as referências do Faria v2 para o resolvedor do Latin Library.

O banco é aberto em modo imutável e somente verbetes públicos são exportados.
O mapa bibliográfico revisado do Faria v3 é reutilizado apenas para interpretar
as abreviações impressas; nenhum conteúdo lexical do v3 participa deste fluxo.
"""

from __future__ import annotations

import argparse
from collections import Counter
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from typing import Any
from urllib.parse import quote

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from dicionarios.scripts.faria_v3.export_corpus_citations import (
    DEFAULT_MAP,
    authority_records,
    canonical_json,
    load_map,
    normalize_alias,
    parse_reference,
)


FORMAT = "faria-v2-citation-export-report-1"
EXPORTER_VERSION = "faria-v2-citation-export-1"
SOURCE_KEY = "faria_v2"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def open_read_only(path: Path) -> sqlite3.Connection:
    absolute = path.resolve()
    if not absolute.is_file():
        raise FileNotFoundError(absolute)
    connection = sqlite3.connect(
        f"file:{quote(str(absolute))}?mode=ro&immutable=1", uri=True
    )
    connection.row_factory = sqlite3.Row
    return connection


def examples(raw_json: str) -> list[str]:
    try:
        payload = json.loads(raw_json)
    except (TypeError, json.JSONDecodeError):
        return []
    values = payload.get("exemplos") if isinstance(payload, dict) else None
    if not isinstance(values, list):
        return []
    return [value.strip() for value in values if isinstance(value, str) and value.strip()]


def lemma_candidates(raw_json: str, lemma: str, lemma_sort: str) -> list[str]:
    values: list[str] = [lemma, lemma_sort]
    try:
        payload = json.loads(raw_json)
    except (TypeError, json.JSONDecodeError):
        payload = {}
    raw_values = payload.get("lemas") if isinstance(payload, dict) else None
    if isinstance(raw_values, list):
        values.extend(value for value in raw_values if isinstance(value, str))
    canonical = payload.get("lema_canonico") if isinstance(payload, dict) else None
    if isinstance(canonical, str):
        values.append(canonical)
    result: list[str] = []
    for value in values:
        clean = " ".join(value.strip().split())
        if clean and clean not in result:
            result.append(clean)
    return result


def v2_citation_map(path: Path) -> dict[str, Any]:
    citation_map = copy.deepcopy(load_map(path))
    citation_map["source_key"] = SOURCE_KEY
    citation_map["label"] = "Ernesto Faria — Dicionário Escolar Latino-Português (retificado v2)"
    citation_map["version"] = f"{citation_map['version']}-retificado-v2"
    return citation_map


def export(
    database: Path,
    map_path: Path,
    claims_path: Path,
    authorities_path: Path,
    report_path: Path,
) -> dict[str, Any]:
    citation_map = v2_citation_map(map_path)
    database_sha256 = file_sha256(database)
    counts: Counter[str] = Counter()
    unknown_prefixes: Counter[str] = Counter()
    public_entries = 0
    redirect_entries = 0
    ordinal_by_document: Counter[str] = Counter()

    claims_path.parent.mkdir(parents=True, exist_ok=True)
    with open_read_only(database) as connection, claims_path.open(
        "w", encoding="utf-8", newline="\n"
    ) as target:
        for row in connection.execute(
            "SELECT entry_id,id,lemma,lemma_sort,redirect_only,raw_json "
            "FROM entry ORDER BY entry_id"
        ):
            legacy_id = str(row["id"])
            document = legacy_id.split(":", 1)[0]
            ordinal_by_document[document] += 1
            if int(row["redirect_only"]):
                redirect_entries += 1
                continue
            public_entries += 1
            site_entry_id = f"{document}-e{ordinal_by_document[document]}"
            for ordinal, raw_reference in enumerate(examples(row["raw_json"]), 1):
                parsed = parse_reference(raw_reference, citation_map)
                status = str(parsed["status"])
                counts[status] += 1
                if status in {
                    "unknown_author",
                    "unknown_work",
                    "ambiguous_prefix",
                    "invalid_reference",
                }:
                    unknown_prefixes[str(parsed.get("raw_prefix") or raw_reference)] += 1
                record = {
                    "source_key": SOURCE_KEY,
                    "source_citation_id": f"{legacy_id}:c{ordinal:03d}",
                    "source_entry_id": legacy_id,
                    "site_entry_id": site_entry_id,
                    "context_ordinal": ordinal,
                    "lemma_raw": row["lemma"],
                    "lemma_norm": row["lemma_sort"],
                    "v2_lemma_candidates": lemma_candidates(
                        row["raw_json"], str(row["lemma"]), str(row["lemma_sort"])
                    ),
                    "author_raw": parsed.get("author_raw"),
                    "work_raw": parsed.get("work_raw"),
                    "reference_raw": parsed.get("reference_raw") or raw_reference,
                    "quote": None,
                    "claim_kind": "reference",
                    "faria_reference_raw": raw_reference,
                    "parse_status": status,
                    "parse_evidence": parsed,
                    "provenance_role": "retificado_v2_public_entry",
                    "v2_entry_id": int(row["entry_id"]),
                }
                target.write(canonical_json(record) + "\n")

    authorities = authority_records(citation_map, database_sha256)
    # Record explicitly that the map is shared while the lexical source is not.
    authorities[0]["metadata"]["exporter_version"] = EXPORTER_VERSION
    authorities[0]["metadata"]["lexical_source"] = "retificado_v2.db"
    authorities_path.parent.mkdir(parents=True, exist_ok=True)
    authorities_path.write_text(
        "".join(canonical_json(record) + "\n" for record in authorities),
        encoding="utf-8",
    )

    report = {
        "format": FORMAT,
        "exporter_version": EXPORTER_VERSION,
        "source_key": SOURCE_KEY,
        "input_database": str(database.resolve()),
        "input_sha256": database_sha256,
        "citation_map": str(map_path.resolve()),
        "citation_map_sha256": file_sha256(map_path),
        "public_entries": public_entries,
        "redirect_entries_excluded": redirect_entries,
        "claims": sum(counts.values()),
        "parse_status": dict(sorted(counts.items())),
        "top_unknown_prefixes": unknown_prefixes.most_common(100),
        "claims_output": str(claims_path.resolve()),
        "authorities_output": str(authorities_path.resolve()),
        "contracts": {
            "input_database_read_only_immutable": True,
            "public_entries_only": True,
            "uses_v3_lexical_content": False,
            "quotes_invented": False,
        },
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("database", type=Path)
    result.add_argument("--map", dest="map_path", type=Path, default=DEFAULT_MAP)
    result.add_argument("--claims", type=Path, required=True)
    result.add_argument("--authorities", type=Path, required=True)
    result.add_argument("--report", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        report = export(
            args.database,
            args.map_path,
            args.claims,
            args.authorities,
            args.report,
        )
    except (FileNotFoundError, ValueError, sqlite3.Error) as error:
        print(f"erro: {error}")
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
