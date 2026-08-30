#!/usr/bin/env python3
"""Export Faria citation claims for the deterministic TLL resolver.

The staging database remains read-only.  Bibliographic abbreviations are
interpreted from a reviewed, versioned map derived from the printed Faria
abbreviation list.  Unknown or catalog-less references are exported too, but
without pretending that they have a resolvable author/work pair.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import unicodedata
from typing import Any


HERE = Path(__file__).resolve().parent
DEFAULT_MAP = HERE / "faria_citation_map_v1.json"
EXPORTER_VERSION = "faria-citation-export-1"


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def normalize_alias(value: str | None) -> str:
    folded = unicodedata.normalize("NFKD", value or "").casefold()
    folded = "".join(char for char in folded if not unicodedata.combining(char))
    folded = folded.replace("æ", "ae").replace("œ", "oe")
    return " ".join(re.findall(r"[a-z0-9]+", folded))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_map(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("format") != "faria-citation-map-v1":
        raise ValueError(f"{path}: unsupported citation map format")
    if not isinstance(value.get("authors"), list):
        raise ValueError(f"{path}: authors must be an array")
    return value


def _strip_outer_parentheses(value: str) -> str:
    text = " ".join(value.strip().split())
    while text.startswith("(") and text.endswith(")"):
        text = text[1:-1].strip()
    return text


def _prefix_and_locator(reference: str) -> tuple[str, str] | None:
    text = _strip_outer_parentheses(reference)
    match = re.search(r"\d", text)
    if match is None:
        return None
    prefix = text[: match.start()].strip(" ,;:-")
    locator = text[match.start() :].strip()
    return (prefix, locator) if prefix and locator else None


def _matches_prefix(normalized_prefix: str, normalized_alias: str) -> bool:
    return normalized_prefix == normalized_alias or normalized_prefix.startswith(
        normalized_alias + " "
    )


def parse_reference(reference: str, citation_map: dict[str, Any]) -> dict[str, Any]:
    """Split one printed citation into a reviewed author/work/locator triple."""

    split = _prefix_and_locator(reference)
    if split is None:
        return {"status": "invalid_reference", "raw_reference": reference}
    raw_prefix, locator = split
    prefix = normalize_alias(raw_prefix)
    candidates: list[dict[str, Any]] = []
    author_seen = False
    for author in citation_map["authors"]:
        for author_alias in author["aliases"]:
            normalized_author = normalize_alias(author_alias)
            if not _matches_prefix(prefix, normalized_author):
                continue
            author_seen = True
            remainder = prefix[len(normalized_author) :].strip()
            for work in author.get("works", []):
                aliases = work.get("aliases", [])
                is_default = bool(work.get("default"))
                matched_alias = next(
                    (alias for alias in aliases if normalize_alias(alias) == remainder), None
                )
                if matched_alias is None and not (is_default and not remainder):
                    continue
                candidates.append(
                    {
                        "status": (
                            "parsed" if author.get("entity_key") and work.get("entity_key")
                            else "catalog_gap"
                        ),
                        "raw_reference": reference,
                        "raw_prefix": raw_prefix,
                        "author_key": author["key"],
                        "work_key": work["key"],
                        "author_entity_key": author.get("entity_key"),
                        "work_entity_key": work.get("entity_key"),
                        "author_raw": author["resolver_alias"],
                        "work_raw": work["resolver_alias"],
                        "reference_raw": locator,
                        "work_inferred": matched_alias is None,
                    }
                )
    unique = {
        (item["author_key"], item["work_key"], item["reference_raw"]): item
        for item in candidates
    }
    if len(unique) == 1:
        return next(iter(unique.values()))
    if len(unique) > 1:
        return {
            "status": "ambiguous_prefix",
            "raw_reference": reference,
            "raw_prefix": raw_prefix,
            "candidate_keys": [
                f"{item['author_key']}:{item['work_key']}"
                for item in sorted(unique.values(), key=lambda item: (item["author_key"], item["work_key"]))
            ],
        }
    return {
        "status": "unknown_work" if author_seen else "unknown_author",
        "raw_reference": reference,
        "raw_prefix": raw_prefix,
    }


def authority_records(citation_map: dict[str, Any], staging_sha256: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = [
        {
            "record_type": "source",
            "source_key": citation_map["source_key"],
            "label": citation_map["label"],
            "version": citation_map["version"],
            "source_sha256": staging_sha256,
            "metadata": {
                "citation_map_version": citation_map["version"],
                "exporter_version": EXPORTER_VERSION,
                "abbreviation_evidence": citation_map.get("evidence", []),
            },
        }
    ]
    aliases_seen: set[tuple[str, str, str, str | None]] = set()
    schemes_seen: set[tuple[str, str]] = set()
    for author in citation_map["authors"]:
        author_key = author.get("entity_key")
        if not author_key:
            continue
        alias_key = ("author", normalize_alias(author["resolver_alias"]), author_key, None)
        if alias_key not in aliases_seen:
            aliases_seen.add(alias_key)
            records.append(
                {
                    "record_type": "alias",
                    "source_key": citation_map["source_key"],
                    "alias_kind": "author",
                    "alias": author["resolver_alias"],
                    "target_entity_key": author_key,
                    "review_status": "reviewed",
                    "evidence": citation_map.get("evidence", []),
                }
            )
        for work in author.get("works", []):
            work_key = work.get("entity_key")
            if not work_key:
                continue
            work_alias_key = (
                "work",
                normalize_alias(work["resolver_alias"]),
                work_key,
                author_key,
            )
            if work_alias_key not in aliases_seen:
                aliases_seen.add(work_alias_key)
                records.append(
                    {
                        "record_type": "alias",
                        "source_key": citation_map["source_key"],
                        "alias_kind": "work",
                        "alias": work["resolver_alias"],
                        "target_entity_key": work_key,
                        "context_entity_key": author_key,
                        "review_status": "reviewed",
                        "evidence": citation_map.get("evidence", []),
                    }
                )
            scheme = work.get("scheme")
            if scheme and (work_key, scheme["scheme_key"]) not in schemes_seen:
                schemes_seen.add((work_key, scheme["scheme_key"]))
                records.append(
                    {
                        "record_type": "scheme",
                        # Schemes describe the works, not one dictionary's spelling.
                        "target_entity_key": work_key,
                        "scheme_key": scheme["scheme_key"],
                        "levels": scheme["levels"],
                        "metadata": scheme.get("metadata", {}),
                    }
                )
    return records


def _safe_quotes(connection: sqlite3.Connection) -> dict[int, str]:
    """Return only deterministic Latin spans, never raw Portuguese glosses."""

    result: dict[int, str] = {}
    rows = connection.execute(
        "SELECT c.citation_id,c.quote_raw,c.translation_raw,n.value_norm,n.evidence_json "
        "FROM citation_claim c LEFT JOIN nlp_evidence n "
        "ON n.scope_type='citation' AND n.scope_id=CAST(c.citation_id AS TEXT) "
        "AND n.analyzer_code='deterministic_faria_nlp' "
        "AND n.analysis_kind='printed_example_segmentation' "
        "WHERE c.claim_kind='example' AND c.quote_raw IS NOT NULL "
        "AND trim(c.quote_raw)<>''"
    )
    for row in rows:
        quote: str | None = None
        evidence = json.loads(row["evidence_json"]) if row["evidence_json"] else {}
        if row["value_norm"] == "explicit_delimiter":
            quote = str(evidence.get("latin_span") or "").strip()
        elif row["translation_raw"] and str(row["translation_raw"]).strip():
            quote = str(row["quote_raw"]).strip()
        if quote and len(re.findall(r"[^\W\d_]+", quote, re.UNICODE)) >= 2:
            result[int(row["citation_id"])] = quote
    return result


def export_claims(
    staging: Path,
    citation_map: dict[str, Any],
    output: Path,
    report_output: Path,
    map_path: Path = DEFAULT_MAP,
) -> dict[str, Any]:
    connection = sqlite3.connect(f"file:{staging.resolve()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    counts: Counter[str] = Counter()
    unknown_prefixes: Counter[str] = Counter()
    catalog_gaps: Counter[str] = Counter()
    safe_quotes = _safe_quotes(connection)
    query = (
        "SELECT c.*,f.headword_raw,h.form lemma_norm "
        "FROM citation_claim c JOIN entry_fragment f ON f.fragment_id=c.fragment_id "
        "LEFT JOIN entry_headword h ON h.fragment_id=c.fragment_id AND h.is_primary=1 "
        "ORDER BY c.fragment_id,c.sequence_no,c.citation_id"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="\n") as target:
        for row in connection.execute(query):
            raw_reference = str(row["reference_raw"] or "").strip()
            parsed = parse_reference(raw_reference, citation_map)
            status = str(parsed["status"])
            counts[status] += 1
            if status in {"unknown_author", "unknown_work", "ambiguous_prefix", "invalid_reference"}:
                unknown_prefixes[str(parsed.get("raw_prefix") or raw_reference)] += 1
            elif status == "catalog_gap":
                catalog_gaps[f"{parsed['author_key']}:{parsed['work_key']}"] += 1
            record: dict[str, Any] = {
                "source_key": citation_map["source_key"],
                "source_citation_id": f"{row['fragment_id']}:c{int(row['sequence_no']):03d}",
                "source_entry_id": row["fragment_id"],
                "context_ordinal": int(row["sequence_no"]),
                "lemma_raw": row["headword_raw"],
                "lemma_norm": row["lemma_norm"],
                "author_raw": parsed.get("author_raw"),
                "work_raw": parsed.get("work_raw"),
                "reference_raw": parsed.get("reference_raw") or raw_reference,
                "quote": safe_quotes.get(int(row["citation_id"])),
                "claim_kind": row["claim_kind"],
                "faria_reference_raw": raw_reference,
                "faria_quote_raw": row["quote_raw"],
                "faria_translation_raw": row["translation_raw"],
                "parse_status": status,
                "parse_evidence": parsed,
                "provenance_role": row["provenance_role"],
                "staging_citation_id": int(row["citation_id"]),
            }
            target.write(canonical_json(record) + "\n")
    connection.close()
    report = {
        "format": "faria-citation-export-report-v1",
        "exporter_version": EXPORTER_VERSION,
        "source_key": citation_map["source_key"],
        "input_database": staging.name,
        "input_sha256": file_sha256(staging),
        "citation_map": map_path.name,
        "citation_map_sha256": file_sha256(map_path),
        "claims": sum(counts.values()),
        "safe_quotes": len(safe_quotes),
        "parse_status": dict(sorted(counts.items())),
        "top_unknown_prefixes": unknown_prefixes.most_common(100),
        "top_catalog_gaps": catalog_gaps.most_common(100),
    }
    report_output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(canonical_json(record) + "\n" for record in records), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("staging", type=Path)
    parser.add_argument("--map", dest="map_path", type=Path, default=DEFAULT_MAP)
    parser.add_argument("--claims", type=Path, required=True)
    parser.add_argument("--authorities", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    citation_map = load_map(args.map_path)
    report = export_claims(
        args.staging, citation_map, args.claims, args.report, args.map_path
    )
    write_jsonl(
        args.authorities,
        authority_records(citation_map, str(report["input_sha256"])),
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
