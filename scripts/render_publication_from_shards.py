#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Renderiza artefatos públicos a partir de shards NDJSON multi-volume.

Entrada:
  - catalog.json (lista de volumes)
  - resultados/shards/<vol>/index.json + shard_*.ndjson

Saída (por volume em web/public/data/<vol>/):
  - meta/<vol>-block-<NNNNN>.json (blocos sequenciais com N entradas cada)
  - meta/<vol>.json (índice leve com blocos)
  - dict/morfologia.json (dedup morfologia por volume)
  - dict/lemmas-first-letter.json (contagem por inicial)
  - dict/lemmas-top.json (top 500 lemas para sugestões)
  - lookup/<vol>-id-to-block.json (mapa id→{block_file, idx_in_block} para o viewer)
E um catálogo geral:
  - web/public/data/volumes.json

Uso:
  python scripts/render_publication_from_shards.py \\
      --catalog resultados/catalog.json \\
      --shards resultados/shards \\
      --out web/public/data \\
      --block-size 500
"""
from __future__ import annotations

import argparse
import json
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Dict, List


def normalize_first_letter(s: str) -> str:
    if not s:
        return "#"
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s[0].upper()


def load_catalog(path: Path) -> List[Dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("catalog.json deve ser uma lista")
    return data


def load_shards(volume_id: str, shards_dir: Path):
    index_path = shards_dir / volume_id / "index.json"
    meta = json.loads(index_path.read_text(encoding="utf-8"))
    entries: List[Dict] = []
    for shard in meta["shards"]:
        shard_path = shards_dir / volume_id / shard["file"]
        with shard_path.open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                entries.append(json.loads(line))
    entries.sort(key=lambda e: e.get("page_num", 0))
    return entries, meta


def write_blocks(volume_id: str, out_base: Path, entries: List[Dict], block_size: int):
    """Divide entradas em blocos sequenciais e retorna (block_records, lookup).

    block_records: lista de dicts compatíveis com meta/<vol>.json
    lookup: mapa {id: {block_file, idx_in_block}} para lookup/<vol>-id-to-block.json
    """
    out_meta_dir = out_base / "meta"
    out_meta_dir.mkdir(parents=True, exist_ok=True)
    block_records = []
    lookup: Dict[str, Dict] = {}

    for block_num, start_idx in enumerate(range(0, len(entries), block_size), start=1):
        block_items = entries[start_idx: start_idx + block_size]
        fname = f"{volume_id}-block-{block_num:05d}.json"
        path = out_meta_dir / fname
        path.write_text(
            json.dumps(block_items, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        block_file = f"meta/{fname}"
        block_records.append(
            {
                "start_idx": start_idx + 1,
                "end_idx": start_idx + len(block_items),
                "file": block_file,
                "count": len(block_items),
            }
        )
        for local_idx, entry in enumerate(block_items):
            entry_id = entry.get("id")
            if entry_id:
                lookup[entry_id] = {"block_file": block_file, "idx_in_block": local_idx}

    return block_records, lookup


def write_dicts(volume_id: str, out_base: Path, entries: List[Dict]):
    out_dict_dir = out_base / "dict"
    out_dict_dir.mkdir(parents=True, exist_ok=True)

    morfs = sorted(
        {e.get("morfologia") for e in entries if e.get("morfologia")}, key=str
    )
    (out_dict_dir / "morfologia.json").write_text(
        json.dumps(morfs, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    counter = Counter()
    for e in entries:
        lemmas = e.get("lemmas") or []
        if not lemmas:
            continue
        counter[normalize_first_letter(lemmas[0])] += 1

    (out_dict_dir / "lemmas-first-letter.json").write_text(
        json.dumps(counter, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Top lemas (para sugestões)
    top_counter = Counter()
    for e in entries:
        for l in e.get("lemmas") or []:
            if l:
                top_counter[l] += 1
    top_items = [
        {"label": lemma, "count": cnt}
        for lemma, cnt in top_counter.most_common(500)
    ]
    (out_dict_dir / "lemmas-top.json").write_text(
        json.dumps({"items": top_items}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_lookup(volume_id: str, out_base: Path, lookup: Dict):
    out_lookup = out_base / "lookup"
    out_lookup.mkdir(parents=True, exist_ok=True)
    (out_lookup / f"{volume_id}-id-to-block.json").write_text(
        json.dumps(lookup, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Renderiza artefatos públicos a partir de shards NDJSON."
    )
    parser.add_argument(
        "--catalog",
        default="resultados/catalog.json",
        help="Caminho do catálogo (default: resultados/catalog.json)",
    )
    parser.add_argument(
        "--shards",
        default="resultados/shards",
        help="Diretório raiz dos shards (default: resultados/shards)",
    )
    parser.add_argument(
        "--out",
        default="web/public/data",
        help="Diretório de saída para artefatos públicos (default: web/public/data)",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=500,
        help="Número de entradas por bloco (default: 500)",
    )
    args = parser.parse_args()

    catalog_path = Path(args.catalog).resolve()
    shards_dir = Path(args.shards).resolve()
    out_root = Path(args.out).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    catalog = load_catalog(catalog_path)
    volumes_public = []

    for volume in catalog:
        volume_id = volume["id"]
        title = volume.get("title", volume_id)
        entries, _shards_meta = load_shards(volume_id, shards_dir)

        vol_base = out_root / volume_id
        block_records, lookup = write_blocks(volume_id, vol_base, entries, args.block_size)
        write_dicts(volume_id, vol_base, entries)
        write_lookup(volume_id, vol_base, lookup)

        page_min = min(e["page_num"] for e in entries) if entries else None
        page_max = max(e["page_num"] for e in entries) if entries else None
        meta_obj = {
            "volume_id": volume_id,
            "title": title,
            "page_min": page_min,
            "page_max": page_max,
            "blocks": block_records,
        }
        meta_path = vol_base / "meta" / f"{volume_id}.json"
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(
            json.dumps(meta_obj, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        volumes_public.append(
            {
                "volume_id": volume_id,
                "title": title,
                "page_min": page_min,
                "page_max": page_max,
                "meta_url": f"data/{volume_id}/meta/{volume_id}.json",
                "blocks_prefix": f"data/{volume_id}/",
            }
        )

        print(
            f"[{volume_id}] {len(entries)} registros -> {len(block_records)} blocos; "
            f"lookup com {len(lookup)} entradas"
        )

    # volumes.json
    volumes_json = out_root / "volumes.json"
    volumes_json.write_text(
        json.dumps(volumes_public, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Gerado catálogo público em {volumes_json}")


if __name__ == "__main__":
    main()
