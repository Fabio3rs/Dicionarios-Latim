#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera shards NDJSON multi-volume a partir de um catálogo de volumes.

Entrada:
  - catalog.json: lista de volumes {id, title, source_json, shard_size?}
  - Cada source_json segue o formato de normalized_results.json já usado no projeto.

Saídas (por volume):
  resultados/shards/<vol>/index.json
  resultados/shards/<vol>/shard_00001.ndjson, shard_00002.ndjson, ...

Uso típico:
  python scripts/export_shards_from_normalized.py \\
         --catalog resultados/catalog.json \\
         --outdir resultados/shards \\
         --shard-size 1000

Obs.:
  - Cada linha do NDJSON representa uma entrada lexical (flatten) com volume_id.
  - O index.json inclui metadados de páginas e os arquivos gerados.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List


def flatten_record(rec: Dict, volume_id: str) -> Iterable[Dict]:
    """
    Achata um item de normalized_results (que pode conter vários verbetes)
    gerando um dicionário por verbete, incluindo volume_id.
    """
    doc_name = rec.get("doc_name")
    page_num = rec.get("page_num")
    raw_text = rec.get("raw_text") or ""
    entries = rec.get("normalized_text") or []

    for i, item in enumerate(entries, start=1):
        yield {
            "id": f"{doc_name}-e{i}",
            "volume_id": volume_id,
            "doc_name": doc_name,
            "page_num": page_num,
            "lemmas": item.get("lemas") or [],
            "morfologia": item.get("morfologia"),
            "definicao": item.get("definicao"),
            "exemplos": item.get("exemplos") or [],
            "notas": item.get("notas"),
            "raw_text": raw_text,
        }


def chunked(seq: List[Dict], size: int) -> Iterable[List[Dict]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def build_index(shards: List[Dict], shard_size: int, total_records: int, pages) -> Dict:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "shard_size": shard_size,
        "total_records": total_records,
        "page_min": min(pages) if pages else None,
        "page_max": max(pages) if pages else None,
        "shards": shards,
    }


def load_catalog(path: Path) -> List[Dict]:
    items = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(items, list):
        raise ValueError("catalog.json deve ser uma lista de volumes")
    return items


def resolve_source_path(raw: str, base_dir: Path) -> Path:
    cand = Path(raw)
    if cand.is_absolute():
        return cand
    candidates = [
        base_dir / cand,
        base_dir.parent / cand,
        Path.cwd() / cand,
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError(f"source_json não encontrado: {raw}")


def process_volume(volume: Dict, outdir: Path, default_shard_size: int, base_dir: Path) -> Dict:
    volume_id = volume["id"]
    shard_size = int(volume.get("shard_size") or default_shard_size)
    source_path = resolve_source_path(volume["source_json"], base_dir)

    data = json.loads(source_path.read_text(encoding="utf-8"))
    pages = [item.get("page_num") for item in data]

    flattened: List[Dict] = []
    for rec in data:
        flattened.extend(list(flatten_record(rec, volume_id=volume_id)))

    volume_out = outdir / volume_id
    volume_out.mkdir(parents=True, exist_ok=True)

    shards_meta: List[Dict] = []
    for idx, chunk in enumerate(chunked(flattened, shard_size), start=1):
        fname = f"shard_{idx:05d}.ndjson"
        target = volume_out / fname
        target.write_text(
            "\n".join(json.dumps(row, ensure_ascii=False) for row in chunk),
            encoding="utf-8",
        )
        shards_meta.append(
            {
                "file": fname,
                "count": len(chunk),
                "first_id": chunk[0]["id"],
                "last_id": chunk[-1]["id"],
                "page_min": min(r["page_num"] for r in chunk),
                "page_max": max(r["page_num"] for r in chunk),
            }
        )

    index = build_index(
        shards=shards_meta,
        shard_size=shard_size,
        total_records=len(flattened),
        pages=pages,
    )
    (volume_out / "index.json").write_text(
        json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(
        f"[{volume_id}] {len(shards_meta)} shard(s) com {len(flattened)} registros "
        f"({index['page_min']}–{index['page_max']})."
    )
    return {
        "id": volume_id,
        "total_records": len(flattened),
        "shards": len(shards_meta),
        "page_min": index["page_min"],
        "page_max": index["page_max"],
        "outdir": str(volume_out),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Exporta normalized_results em shards NDJSON multi-volume."
    )
    parser.add_argument(
        "--catalog",
        default="resultados/catalog.json",
        help="Caminho do catálogo de volumes (default: resultados/catalog.json)",
    )
    parser.add_argument(
        "--outdir",
        default="resultados/shards",
        help="Diretório raiz de saída para index + NDJSON (default: resultados/shards)",
    )
    parser.add_argument(
        "--shard-size",
        type=int,
        default=1000,
        help="Número de registros por shard NDJSON (pode ser sobrescrito por volume) (default: 1000)",
    )
    args = parser.parse_args()

    catalog_path = Path(args.catalog).resolve()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    catalog = load_catalog(catalog_path)
    summary = []
    for volume in catalog:
        summary.append(process_volume(volume, outdir, args.shard_size, catalog_path.parent))

    print("Concluído.")
    for item in summary:
        print(
            f" - {item['id']}: {item['total_records']} registros em {item['shards']} shards "
            f"({item['page_min']}–{item['page_max']}) -> {item['outdir']}"
        )


if __name__ == "__main__":
    main()
