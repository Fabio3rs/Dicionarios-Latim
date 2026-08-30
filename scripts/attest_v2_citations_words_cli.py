#!/usr/bin/env python3
"""Atesta citações resolvidas do Faria v2 com o CLI search do WordsWASM.

O perfil WWDB ``search`` não contém definições. A atestação compara os IDs de
lexema retornados para o lema do verbete e para as formas presentes na passagem.
Todas as consultas são enviadas a uma única instância via ``--batch-json-lines``.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import unicodedata
from typing import Any, Iterable


FORMAT = "faria-v2-wordswasm-attestation-report-1"
TOKEN_PATTERN = re.compile(r"[^\W\d_]+", re.UNICODE)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def normalized_letters(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value.casefold())
    letters = [
        character
        for character in decomposed
        if unicodedata.category(character).startswith("L")
    ]
    return unicodedata.normalize("NFC", "".join(letters))


def lemma_queries(row: sqlite3.Row) -> list[str]:
    metadata = json.loads(row["metadata_json"] or "{}")
    values: list[str] = [str(row["lemma_raw"] or ""), str(row["lemma_normalized"] or "")]
    candidates = metadata.get("v2_lemma_candidates")
    if isinstance(candidates, list):
        values.extend(value for value in candidates if isinstance(value, str))
    result: list[str] = []
    for value in values:
        clean = normalized_letters(value)
        if clean and clean not in result:
            result.append(clean)
    return result


def passage_segments(
    connection: sqlite3.Connection, row: sqlite3.Row
) -> list[tuple[int, str, int, int]]:
    start_id = int(row["start_text_unit_id"])
    end_id = int(row["end_text_unit_id"])
    bounds = connection.execute(
        "SELECT min(CASE WHEN id=? THEN sequence_no END),"
        "max(CASE WHEN id=? THEN sequence_no END),catalog_entity_id "
        "FROM text_unit WHERE id IN (?,?)",
        (start_id, end_id, start_id, end_id),
    ).fetchone()
    if bounds is None or bounds[0] is None or bounds[1] is None:
        return []
    units = connection.execute(
        "SELECT id,normalized_text FROM text_unit WHERE catalog_entity_id=? "
        "AND sequence_no BETWEEN ? AND ? ORDER BY sequence_no,id",
        (bounds[2], bounds[0], bounds[1]),
    ).fetchall()
    result: list[tuple[int, str, int, int]] = []
    for unit in units:
        text = str(unit["normalized_text"])
        start = int(row["start_unit_char"] or 0) if int(unit["id"]) == start_id else 0
        end = (
            int(row["end_unit_char"])
            if int(unit["id"]) == end_id and row["end_unit_char"] is not None
            else len(text)
        )
        result.append((int(unit["id"]), text, start, end))
    return result


def analyze_batch(
    executable: Path,
    database: Path,
    dataset_id: str,
    queries: Iterable[str],
) -> dict[str, set[int]]:
    ordered = sorted(set(queries))
    command = [
        str(executable.resolve()),
        "--database",
        str(database.resolve()),
        "--dataset-id",
        dataset_id,
        "--format",
        "search",
        "--batch-json-lines",
    ]
    completed = subprocess.run(
        command,
        input="".join(query + "\n" for query in ordered),
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"words_cli exited with {completed.returncode}: {completed.stderr.strip()}"
        )
    lines = completed.stdout.splitlines()
    if len(lines) != len(ordered):
        raise RuntimeError(
            f"words_cli returned {len(lines)} records for {len(ordered)} queries"
        )
    result: dict[str, set[int]] = {}
    for query, line in zip(ordered, lines, strict=True):
        payload = json.loads(line)
        if payload.get("query", {}).get("text") != query:
            raise RuntimeError(f"words_cli output order mismatch for {query!r}")
        result[query] = {
            int(hit["lexemeId"])
            for hit in payload.get("hits", [])
            if hit.get("lexemeId") is not None
        }
    return result


def attest(
    corpus_database: Path,
    executable: Path,
    words_database: Path,
    manifest_path: Path,
    report_path: Path,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dataset_id = str(manifest.get("datasetId") or "")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", dataset_id):
        raise ValueError("manifest has no valid datasetId")
    expected = manifest.get("files", {}).get("words-search.wwdb", {})
    database_sha256 = sha256_file(words_database)
    if expected.get("sha256") != database_sha256:
        raise ValueError("search WWDB hash differs from manifest")

    connection = sqlite3.connect(corpus_database)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        "SELECT r.id resolution_id,d.lemma_raw,d.lemma_normalized,d.metadata_json,p.* "
        "FROM active_citation_resolution r "
        "JOIN dictionary_citation d ON d.id=r.dictionary_citation_id "
        "JOIN citation_source s ON s.id=d.citation_source_id "
        "JOIN passage p ON p.id=r.passage_id "
        "WHERE s.source_key='faria_v2' AND r.status='exact' ORDER BY r.id"
    ).fetchall()

    segments_by_passage: dict[int, list[tuple[int, str, int, int]]] = {}
    tokens_by_passage: dict[int, list[tuple[str, int, int, int, str]]] = {}
    all_queries: set[str] = set()
    lemma_queries_by_resolution: dict[int, list[str]] = {}
    for row in rows:
        resolution_id = int(row["resolution_id"])
        queries = lemma_queries(row)
        lemma_queries_by_resolution[resolution_id] = queries
        all_queries.update(queries)
        passage_id = int(row["id"])
        if passage_id in tokens_by_passage:
            continue
        segments = passage_segments(connection, row)
        segments_by_passage[passage_id] = segments
        tokens: list[tuple[str, int, int, int, str]] = []
        for unit_id, text, start, end in segments:
            for match in TOKEN_PATTERN.finditer(text[start:end]):
                surface = match.group(0)
                query = normalized_letters(surface)
                if not query:
                    continue
                tokens.append(
                    (surface, unit_id, start + match.start(), start + match.end(), query)
                )
                all_queries.add(query)
        tokens_by_passage[passage_id] = tokens

    analyses = analyze_batch(executable, words_database, dataset_id, all_queries)
    counts: Counter[str] = Counter()
    with connection:
        for row in rows:
            resolution_id = int(row["resolution_id"])
            lemma_ids = set().union(
                *(analyses.get(query, set()) for query in lemma_queries_by_resolution[resolution_id])
            )
            if not lemma_ids:
                counts["lemma_unknown"] += 1
                continue
            passage_id = int(row["id"])
            candidate = next(
                (
                    (surface, unit_id, start, end, query, lemma_ids & analyses.get(query, set()))
                    for surface, unit_id, start, end, query in tokens_by_passage[passage_id]
                    if lemma_ids & analyses.get(query, set())
                ),
                None,
            )
            if candidate is None:
                counts["unattested"] += 1
                continue
            surface, unit_id, start, end, query, matched_ids = candidate
            lemma_norm = normalized_letters(str(row["lemma_normalized"] or row["lemma_raw"] or ""))
            relation = "exact" if query == lemma_norm else "inflection"
            metadata = {
                "analyzer": "WordsWASM words_cli",
                "dataset_id": dataset_id,
                "wwdb_profile": "search-only",
                "wwdb_sha256": database_sha256,
                "lexeme_ids": sorted(matched_ids),
                "lemma_queries": lemma_queries_by_resolution[resolution_id],
            }
            connection.execute(
                "INSERT INTO lexical_attestation "
                "(citation_resolution_id,text_unit_id,char_start,char_end,surface,"
                "lemma_normalized,relation_type,evidence_kind,evidence_reference,"
                "confidence,metadata_json) VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(citation_resolution_id,text_unit_id,char_start,char_end,"
                "lemma_normalized) DO UPDATE SET surface=excluded.surface,"
                "relation_type=excluded.relation_type,evidence_kind=excluded.evidence_kind,"
                "evidence_reference=excluded.evidence_reference,confidence=excluded.confidence,"
                "metadata_json=excluded.metadata_json,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
                (
                    resolution_id,
                    unit_id,
                    start,
                    end,
                    surface,
                    lemma_norm,
                    relation,
                    "morphological_analyzer",
                    f"WordsWASM:{dataset_id}",
                    1.0,
                    json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                ),
            )
            counts[relation] += 1
    connection.close()

    report = {
        "format": FORMAT,
        "corpus_database": str(corpus_database.resolve()),
        "words_cli": str(executable.resolve()),
        "words_database": str(words_database.resolve()),
        "words_database_sha256": database_sha256,
        "manifest": str(manifest_path.resolve()),
        "dataset_id": dataset_id,
        "exact_resolutions": len(rows),
        "unique_analyzer_queries": len(all_queries),
        "attestation_status": dict(sorted(counts.items())),
        "attested": counts["exact"] + counts["inflection"],
        "contracts": {
            "wwdb_profile": "search-only",
            "single_long_lived_cli_process": True,
            "lexical_identity_by_shared_lexeme_id": True,
            "does_not_copy_words_definitions": True,
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
    result.add_argument("--corpus-db", type=Path, required=True)
    result.add_argument("--words-cli", type=Path, required=True)
    result.add_argument("--words-db", type=Path, required=True)
    result.add_argument("--manifest", type=Path, required=True)
    result.add_argument("--report", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        report = attest(
            args.corpus_db,
            args.words_cli,
            args.words_db,
            args.manifest,
            args.report,
        )
    except (FileNotFoundError, ValueError, RuntimeError, sqlite3.Error, json.JSONDecodeError) as error:
        print(f"erro: {error}")
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
