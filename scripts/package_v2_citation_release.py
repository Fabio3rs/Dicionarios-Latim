#!/usr/bin/env python3
"""Empacota os artefatos de citações do Faria v2 para uma GitHub Release."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import sqlite3
import tarfile
from typing import Any


FORMAT = "faria-v2-citations-release-1"
DEFAULT_EPOCH = 1755651872


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_json(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def database_metrics(path: Path) -> dict[str, Any]:
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro&immutable=1", uri=True) as connection:
        return {
            "quick_check": connection.execute("PRAGMA quick_check").fetchone()[0],
            "foreign_key_violations": len(
                connection.execute("PRAGMA foreign_key_check").fetchall()
            ),
            "claims": connection.execute("SELECT count(*) FROM citation_claim").fetchone()[0],
            "published_citations": connection.execute(
                "SELECT count(*) FROM published_citation "
                "WHERE outcome_status='exact' AND json_array_length(attestations_json)>0"
            ).fetchone()[0],
            "outcomes": dict(
                connection.execute(
                    "SELECT outcome_status,count(*) FROM citation_outcome "
                    "GROUP BY outcome_status ORDER BY outcome_status"
                )
            ),
        }


def tar_member(name: str, data: bytes, epoch: int) -> tuple[tarfile.TarInfo, bytes]:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = 0o644
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = epoch
    return info, data


def write_web_archive(
    archive: Path,
    index: Path,
    shards: list[Path],
    epoch: int,
) -> None:
    members: list[tuple[str, bytes]] = [
        ("data/faria/citations.json", index.read_bytes())
    ]
    members.extend(
        (f"data/faria/citations/{path.name}", path.read_bytes()) for path in shards
    )
    archive.parent.mkdir(parents=True, exist_ok=True)
    with archive.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=epoch) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as target:
                for name, data in sorted(members):
                    info, payload = tar_member(name, data, epoch)
                    target.addfile(info, io.BytesIO(payload))


def build(
    citations_database: Path,
    v2_database: Path,
    index: Path,
    shards_dir: Path,
    output_dir: Path,
    tag: str,
    epoch: int,
) -> dict[str, Any]:
    shards = sorted(shards_dir.glob("*.json"))
    index_payload = json.loads(index.read_text(encoding="utf-8"))
    expected_shards = index_payload.get("shards")
    if not isinstance(expected_shards, list) or [path.name for path in shards] != expected_shards:
        raise ValueError("citation index and shard directory differ")
    metrics = database_metrics(citations_database)
    if metrics["quick_check"] != "ok" or metrics["foreign_key_violations"]:
        raise ValueError(f"invalid citations database: {metrics}")

    output_dir.mkdir(parents=True, exist_ok=True)
    release_database = output_dir / "faria-v2-citations.sqlite"
    if release_database.exists():
        if sha256_file(release_database) != sha256_file(citations_database):
            raise ValueError(f"existing release database differs: {release_database}")
    else:
        os.link(citations_database.resolve(), release_database)
    archive = output_dir / "faria-v2-citations-web.tar.gz"
    write_web_archive(archive, index, shards, epoch)
    manifest = {
        "format": FORMAT,
        "release_tag": tag,
        "source_date_epoch": epoch,
        "artifacts": {
            "faria-v2-citations.sqlite": {
                "source_path": release_database.name,
                "bytes": release_database.stat().st_size,
                "sha256": sha256_file(release_database),
                "role": "portable_audit_database",
            },
            archive.name: {
                "bytes": archive.stat().st_size,
                "sha256": sha256_file(archive),
                "role": "static_site_payload",
                "extract_root": "web/public",
            },
        },
        "inputs": {
            "retificado_v2_sha256": sha256_file(v2_database),
            "citation_index_sha256": sha256_file(index),
            "citation_shards": len(shards),
            "citation_shards_sha256": hashlib.sha256(
                b"".join(
                    f"{sha256_file(path)}  {PurePosixPath('citations') / path.name}\n".encode()
                    for path in shards
                )
            ).hexdigest(),
        },
        "database": metrics,
        "contracts": {
            "web_archive_contains_sqlite": False,
            "web_archive_contains_v3_lexical_content": False,
            "web_archive_paths_are_relative": True,
            "archive_metadata_is_deterministic": True,
        },
    }
    manifest_path = output_dir / "faria-v2-citations-manifest.json"
    manifest_path.write_bytes(canonical_json(manifest))
    checksum_path = output_dir / "faria-v2-citations-assets.sha256"
    checksum_path.write_text(
        "".join(
            f"{sha256_file(path)}  {path.name}\n"
            for path in (release_database, archive, manifest_path)
        ),
        encoding="utf-8",
    )
    report = {
        **manifest,
        "manifest_sha256": sha256_file(manifest_path),
        "checksums_sha256": sha256_file(checksum_path),
        "output_dir": str(output_dir.resolve()),
    }
    (output_dir / "faria-v2-citations-release-report.json").write_bytes(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    )
    return report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--citations-db", type=Path, required=True)
    result.add_argument("--v2-db", type=Path, required=True)
    result.add_argument("--index", type=Path, required=True)
    result.add_argument("--shards", type=Path, required=True)
    result.add_argument("--output-dir", type=Path, required=True)
    result.add_argument("--tag", default="data-v2-citations-2026-08-30")
    result.add_argument("--source-date-epoch", type=int, default=DEFAULT_EPOCH)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        report = build(
            args.citations_db,
            args.v2_db,
            args.index,
            args.shards,
            args.output_dir,
            args.tag,
            args.source_date_epoch,
        )
    except (FileNotFoundError, ValueError, sqlite3.Error, json.JSONDecodeError) as error:
        print(f"erro: {error}")
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
