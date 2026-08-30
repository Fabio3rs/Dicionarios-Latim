#!/usr/bin/env python3
"""Refina ambiguidades do Faria v2 por equivalência ou morfologia WordsWASM."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any

from attest_v2_citations_words_cli import (
    analyze_batch,
    lemma_queries,
    normalized_letters,
    passage_segments,
    TOKEN_PATTERN,
    sha256_file,
)


FORMAT = "faria-v2-ambiguous-refinement-report-1"


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def passage_signature(row: sqlite3.Row) -> tuple[Any, ...]:
    return (
        row["source_artifact_id"],
        row["start_text_unit_id"],
        row["end_text_unit_id"],
        row["start_unit_char"],
        row["end_unit_char"],
    )


def refine(
    corpus_database: Path,
    executable: Path,
    words_database: Path,
    manifest_path: Path,
    report_path: Path,
) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dataset_id = str(manifest["datasetId"])
    database_sha256 = sha256_file(words_database)
    if manifest["files"]["words-search.wwdb"]["sha256"] != database_sha256:
        raise ValueError("search WWDB hash differs from manifest")

    connection = sqlite3.connect(corpus_database)
    connection.row_factory = sqlite3.Row
    old_run = connection.execute(
        "SELECT DISTINCT r.processing_run_id FROM active_citation_resolution r "
        "JOIN dictionary_citation d ON d.id=r.dictionary_citation_id "
        "JOIN citation_source s ON s.id=d.citation_source_id "
        "WHERE s.source_key='faria_v2'"
    ).fetchall()
    if len(old_run) != 1:
        raise ValueError(f"expected one active Faria v2 run, found {len(old_run)}")
    old_run_id = int(old_run[0][0])
    ambiguous = connection.execute(
        "SELECT r.*,d.lemma_raw,d.lemma_normalized,d.metadata_json "
        "FROM citation_resolution r "
        "JOIN dictionary_citation d ON d.id=r.dictionary_citation_id "
        "JOIN citation_source s ON s.id=d.citation_source_id "
        "WHERE s.source_key='faria_v2' AND r.processing_run_id=? "
        "AND r.status='ambiguous' AND json_array_length(r.candidates_json)>0 "
        "ORDER BY r.id",
        (old_run_id,),
    ).fetchall()

    all_queries: set[str] = set()
    lemma_queries_by_resolution: dict[int, list[str]] = {}
    tokens_by_passage: dict[int, list[str]] = {}
    passages: dict[int, sqlite3.Row] = {}
    equivalent_choice: dict[int, int] = {}
    for row in ambiguous:
        resolution_id = int(row["id"])
        queries = lemma_queries(row)
        lemma_queries_by_resolution[resolution_id] = queries
        all_queries.update(queries)
        candidate_ids = [int(value) for value in json.loads(row["candidates_json"])]
        candidate_rows = [
            connection.execute("SELECT * FROM passage WHERE id=?", (passage_id,)).fetchone()
            for passage_id in candidate_ids
        ]
        candidate_rows = [item for item in candidate_rows if item is not None]
        signatures = {passage_signature(item) for item in candidate_rows}
        if len(candidate_rows) == len(candidate_ids) and len(signatures) == 1:
            equivalent_choice[resolution_id] = min(candidate_ids)
        for passage in candidate_rows:
            passage_id = int(passage["id"])
            passages[passage_id] = passage
            if passage_id in tokens_by_passage:
                continue
            tokens: list[str] = []
            for _, text, start, end in passage_segments(connection, passage):
                for match in TOKEN_PATTERN.finditer(text[start:end]):
                    query = normalized_letters(match.group(0))
                    if query:
                        tokens.append(query)
                        all_queries.add(query)
            tokens_by_passage[passage_id] = tokens

    analyses = analyze_batch(executable, words_database, dataset_id, all_queries)
    promoted: dict[int, tuple[int, str]] = {}
    for row in ambiguous:
        resolution_id = int(row["id"])
        if resolution_id in equivalent_choice:
            promoted[resolution_id] = (
                equivalent_choice[resolution_id],
                "equivalent_passage_candidates",
            )
            continue
        lemma_ids = set().union(
            *(analyses.get(query, set()) for query in lemma_queries_by_resolution[resolution_id])
        )
        if not lemma_ids:
            continue
        matching: list[int] = []
        for passage_id in map(int, json.loads(row["candidates_json"])):
            if any(lemma_ids & analyses.get(token, set()) for token in tokens_by_passage.get(passage_id, [])):
                matching.append(passage_id)
        if len(matching) == 1:
            promoted[resolution_id] = (matching[0], "unique_wordswasm_lexeme_candidate")

    config = {
        "source_key": "faria_v2",
        "parent_resolution_run_id": old_run_id,
        "refiner": "equivalent passage signature or unique WordsWASM lexemeId",
        "dataset_id": dataset_id,
        "wwdb_sha256": database_sha256,
    }
    config_json = stable_json(config)
    config_sha256 = hashlib.sha256(config_json.encode()).hexdigest()
    counts: Counter[str] = Counter()
    with connection:
        cursor = connection.execute(
            "INSERT INTO processing_run "
            "(parent_run_id,run_type,pipeline_name,pipeline_version,config_json,"
            "config_sha256,status,started_at) VALUES "
            "(?,'citation_resolve','faria-v2-wordswasm-refinement','1',?,?,"
            "'running',strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            (old_run_id, config_json, config_sha256),
        )
        new_run_id = int(cursor.lastrowid)
        old_rows = connection.execute(
            "SELECT * FROM citation_resolution WHERE processing_run_id=? ORDER BY id",
            (old_run_id,),
        ).fetchall()
        for row in old_rows:
            status = str(row["status"])
            passage_id = row["passage_id"]
            evidence = json.loads(row["evidence_json"] or "{}")
            choice = promoted.get(int(row["id"]))
            if choice is not None:
                passage_id, reason = choice
                status = "exact"
                evidence["base_reason"] = evidence.get("reason")
                evidence["reason"] = reason
                evidence["refined_from_resolution_id"] = int(row["id"])
                evidence["wordswasm_dataset_id"] = dataset_id
                counts[reason] += 1
            counts[status] += 1
            connection.execute(
                "INSERT INTO citation_resolution "
                "(processing_run_id,dictionary_citation_id,author_entity_id,work_entity_id,"
                "passage_id,status,resolver,normalized_reference,candidates_json,"
                "evidence_json,confidence) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    new_run_id,
                    row["dictionary_citation_id"],
                    row["author_entity_id"],
                    row["work_entity_id"],
                    passage_id,
                    status,
                    "faria-v2-wordswasm-refinement:v1",
                    row["normalized_reference"],
                    row["candidates_json"],
                    stable_json(evidence),
                    1.0 if status == "exact" else row["confidence"],
                ),
            )
        connection.execute(
            "UPDATE processing_run SET status='completed',finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),"
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (new_run_id,),
        )
        connection.execute(
            "UPDATE processing_run SET is_active=0,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
            "WHERE run_type='citation_resolve' AND is_active=1"
        )
        connection.execute(
            "UPDATE processing_run SET is_active=1,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
            "WHERE id=?",
            (new_run_id,),
        )
    connection.close()

    report = {
        "format": FORMAT,
        "parent_run_id": old_run_id,
        "run_id": new_run_id,
        "ambiguous_with_candidates": len(ambiguous),
        "promoted": len(promoted),
        "promoted_by_reason": {
            key: counts[key]
            for key in ("equivalent_passage_candidates", "unique_wordswasm_lexeme_candidate")
        },
        "outcomes": {
            key: counts[key]
            for key in ("exact", "ambiguous", "not_found", "unsupported", "corpus_gap", "invalid")
        },
        "dataset_id": dataset_id,
        "words_database_sha256": database_sha256,
        "contracts": {
            "original_resolution_run_preserved": True,
            "new_run_activated_atomically": True,
            "promotion_requires_equivalent_span_or_unique_lexeme_candidate": True,
        },
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
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
        report = refine(
            args.corpus_db, args.words_cli, args.words_db, args.manifest, args.report
        )
    except (FileNotFoundError, ValueError, RuntimeError, sqlite3.Error, json.JSONDecodeError) as error:
        print(f"erro: {error}")
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
