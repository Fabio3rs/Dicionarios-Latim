#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Renderiza artefatos públicos a partir de shards NDJSON multi-volume.

Entrada:
  - catalog.json (lista de volumes)
  - resultados/shards/<vol>/index.json + shard_*.ndjson

Saída (por volume em web/public/data/<vol>/):
  - meta/<vol>-pages-<start>-<end>.json (blocos por faixa de página)
  - meta/<vol>.json (índice leve com blocos)
  - dict/morfologia.json (dedup morfologia por volume)
  - dict/lemmas-first-letter.json (contagem por inicial)
  - lookup/<vol>-id-to-page.json (mapa id→page_num para o viewer)
E um catálogo geral:
  - web/public/data/volumes.json

Uso:
  python scripts/render_publication_from_shards.py \\
      --catalog resultados/catalog.json \\
      --shards resultados/shards \\
      --out web/public/data \\
      --pages-per-block 200
"""
from __future__ import annotations

import argparse
import json
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List


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


def load_shards(volume_id: str, shards_dir: Path) -> List[Dict]:
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


def build_blocks(entries: List[Dict], pages_per_block: int) -> List[Dict]:
    blocks = []
    if not entries:
        return blocks
    page_min = min(e["page_num"] for e in entries)
    page_max = max(e["page_num"] for e in entries)
    start = (page_min // pages_per_block) * pages_per_block
    end_limit = ((page_max // pages_per_block) + 1) * pages_per_block
    for block_start in range(start, end_limit, pages_per_block):
        block_end = block_start + pages_per_block - 1
        chunk = [
            e for e in entries if block_start <= int(e["page_num"]) <= block_end
        ]
        if not chunk:
            continue
        blocks.append(
            {
                "start": block_start,
                "end": block_end,
                "entries": chunk,
            }
        )
    return blocks


def write_blocks(volume_id: str, out_base: Path, blocks: List[Dict]) -> List[Dict]:
    out_meta_dir = out_base / "meta"
    out_meta_dir.mkdir(parents=True, exist_ok=True)
    block_records = []
    for blk in blocks:
        fname = f"{volume_id}-pages-{blk['start']}-{blk['end']}.json"
        path = out_meta_dir / fname
        path.write_text(
            json.dumps(blk["entries"], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        block_records.append(
            {
                "start": blk["start"],
                "end": blk["end"],
                "file": f"meta/{fname}",
                "count": len(blk["entries"]),
            }
        )
    return block_records


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


def write_lookup(volume_id: str, out_base: Path, entries: List[Dict]):
    out_lookup = out_base / "lookup"
    out_lookup.mkdir(parents=True, exist_ok=True)
    mapping = {e["id"]: e.get("page_num") for e in entries}
    (out_lookup / f"{volume_id}-id-to-page.json").write_text(
        json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8"
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
        "--pages-per-block",
        type=int,
        default=200,
        help="Quantidade de páginas por bloco de meta (default: 200)",
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
        entries, shards_meta = load_shards(volume_id, shards_dir)
        blocks = build_blocks(entries, args.pages_per_block)

        vol_base = out_root / volume_id
        block_records = write_blocks(volume_id, vol_base, blocks)
        write_dicts(volume_id, vol_base, entries)
        write_lookup(volume_id, vol_base, entries)

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
                "id": volume_id,
                "title": title,
                "page_min": page_min,
                "page_max": page_max,
                "meta_url": f"data/{volume_id}/meta/{volume_id}.json",
                "blocks_prefix": f"data/{volume_id}/",
                "base_url": "/Dicionarios-Latim",
            }
        )

        print(
            f"[{volume_id}] {len(entries)} registros -> {len(block_records)} blocos; "
            f"meta em {meta_path}"
        )

    # volumes.json
    volumes_json = out_root / "volumes.json"
    volumes_json.write_text(
        json.dumps(volumes_public, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Gerado catálogo público em {volumes_json}")


if __name__ == "__main__":
    main()
