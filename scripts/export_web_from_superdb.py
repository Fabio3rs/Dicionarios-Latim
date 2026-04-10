#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exporta artefatos para o front diretamente do superdb.sqlite, sem depender dos
shards/normalized_results. Gera blocos sem informação de páginas.

Saídas por volume em web/public/data/<volume_id>/:
  - meta/<vol>-block-<n>.json          (verbetes em blocos ordenados)
  - meta/<vol>.json                    (lista de blocos)
  - dict/morfologia.json               (valores únicos de morfologia)
  - dict/lemmas-first-letter.json      (contagem por inicial)
  - dict/lemmas-top.json               (top lemas)
  - dict/forms-top.json                (top formas flexionadas) [opcional]
  - lookup/<vol>-id-to-block.json      (mapa id -> bloco/posição)
Catálogo geral em web/public/data/volumes.json

Uso:
  python scripts/export_web_from_superdb.py \
      --db dicionarios/superdb.sqlite \
      --out web/public/data \
      --sources 1,2,3,4,5,6,7,8 \
      --block-size 500 \
      --lang-priority pt,en,fr,la
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


def slugify(text: str) -> str:
    if not text:
        return "volume"
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text.lower()).strip("-")
    return text or "volume"


def parse_csv_ids(raw: str) -> List[int]:
    return [int(x) for x in re.split(r"[;,]\s*", raw) if x.strip()]


def load_sources(conn: sqlite3.Connection, wanted: Sequence[int]):
    cur = conn.execute("SELECT id, name FROM source")
    result = []
    for source_id, name in cur.fetchall():
        if source_id in wanted:
            result.append({"id": source_id, "name": name, "slug": slugify(name)})
    missing = set(wanted) - {s["id"] for s in result}
    if missing:
        raise SystemExit(f"Sources não encontrados no banco: {sorted(missing)}")
    return result


def build_morph(entry_row: sqlite3.Row) -> str | None:
    pos = entry_row["pos_std"] or entry_row["pos_raw"] or ""
    gender = entry_row["gender_std"] or entry_row["gender_raw"] or ""
    indecl = bool(entry_row["indeclinable"])
    parts = []
    if pos:
        if pos.upper() == "NOUN":
            parts.append("subst.")
        elif pos.upper() == "ADJ":
            parts.append("adj.")
        elif pos.upper() == "VERB":
            parts.append("verbo")
        else:
            parts.append(pos.lower())
    if gender:
        parts.append(f"({gender})" if len(gender) <= 3 else gender)
    if indecl:
        parts.append("indeclinável")
    if not parts:
        return None
    return " ".join(parts)


def normalize_first_letter(s: str) -> str:
    if not s:
        return "#"
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    ch = s[0].upper()
    return ch if "A" <= ch <= "Z" else "#"


def fetch_forms(conn: sqlite3.Connection, entry_id: int) -> List[str]:
    # Deprecated: mantido por compat, não usado no pipeline otimizado
    rows = conn.execute(
        "SELECT form_norm FROM entry_form WHERE entry_id=? AND form_norm IS NOT NULL",
        (entry_id,),
    ).fetchall()
    return [r[0] for r in rows if r[0]]


def export_volume(conn: sqlite3.Connection, source, out_root: Path, block_size: int, lang_priority: List[str], top_forms_counter: Counter):
    volume_id = source["slug"]
    out_base = out_root / volume_id
    (out_base / "meta").mkdir(parents=True, exist_ok=True)
    (out_base / "dict").mkdir(parents=True, exist_ok=True)
    (out_base / "lookup").mkdir(parents=True, exist_ok=True)

    conn.row_factory = sqlite3.Row
    entries = conn.execute(
        """
        SELECT * FROM entry
        WHERE source_id = ?
        ORDER BY lemma_norm ASC, id ASC
        """,
        (source["id"],),
    ).fetchall()

    # Pré-carrega dados auxiliares para evitar SELECT por item
    priority = {lang: i for i, lang in enumerate(lang_priority)}
    default_priority = len(lang_priority)
    sense_best: Dict[int, Tuple[str, str, int]] = {}
    for s_id, e_id, glang, gloss in conn.execute(
        """
        SELECT s.id, s.entry_id, s.gloss_lang, s.gloss
        FROM sense s
        JOIN entry e ON e.id = s.entry_id
        WHERE e.source_id = ?
        """,
        (source["id"],),
    ):
        pr = priority.get(glang, default_priority)
        if e_id not in sense_best or pr < priority.get(sense_best[e_id][1], default_priority):
            sense_best[e_id] = (gloss or "", glang or "", s_id)

    examples_by_sense: Dict[int, List[str]] = defaultdict(list)
    for s_id, qlat, qother in conn.execute(
        """
        SELECT ex.sense_id, ex.quote_lat, ex.quote_other
        FROM example ex
        JOIN sense s ON s.id = ex.sense_id
        JOIN entry e ON e.id = s.entry_id
        WHERE e.source_id = ?
        """,
        (source["id"],),
    ):
        if len(examples_by_sense[s_id]) < 5:
            if qlat:
                examples_by_sense[s_id].append(qlat)
            elif qother:
                examples_by_sense[s_id].append(qother)

    translations_by_sense: Dict[int, List[str]] = defaultdict(list)
    for s_id, gloss in conn.execute(
        """
        SELECT t.sense_id, t.gloss
        FROM translation t
        JOIN sense s ON s.id = t.sense_id
        JOIN entry e ON e.id = s.entry_id
        WHERE e.source_id = ? AND t.lang IN ('pt','la')
        """,
        (source["id"],),
    ):
        if gloss:
            translations_by_sense[s_id].append(gloss)

    forms_by_entry: Dict[int, List[str]] = defaultdict(list)
    for e_id, form_norm in conn.execute(
        """
        SELECT ef.entry_id, ef.form_norm
        FROM entry_form ef
        JOIN entry e ON e.id = ef.entry_id
        WHERE e.source_id = ? AND ef.form_norm IS NOT NULL
        """,
        (source["id"],),
    ):
        forms_by_entry[e_id].append(form_norm)

    items = []
    morph_values = set()
    letter_counter = Counter()
    lemma_counter = Counter()

    for entry_row in entries:
        entry_id = entry_row["id"]
        lemma = entry_row["lemma"] or ""
        lemmas = [lemma]
        morph = build_morph(entry_row)
        if morph:
            morph_values.add(morph)
        letter_counter[normalize_first_letter(lemma)] += 1
        lemma_counter[lemma] += 1

        extra_json = entry_row["extra_json"] or ""
        extra = {}
        if extra_json:
            try:
                extra = json.loads(extra_json)
            except Exception:
                extra = {}

        morph_extra = extra.get("morph_extra") if isinstance(extra.get("morph_extra"), list) else []
        conf = extra.get("conf")

        definicao, gloss_lang, sense_id = "", "", None
        if entry_id in sense_best:
            definicao, gloss_lang, sense_id = sense_best[entry_id]
        exemplos = examples_by_sense.get(sense_id, [])[:5] if sense_id else []
        translations = translations_by_sense.get(sense_id, [])[:3] if sense_id else []

        forms = forms_by_entry.get(entry_id, [])
        for f in forms:
            top_forms_counter[f] += 1

        item = {
            "id": str(entry_id),
            "volume_id": volume_id,
            "lemmas": lemmas,
            "morfologia": morph,
            "morph_render": morph,
            "morph_extra": morph_extra,
            "conf": conf,
            "needs_review": entry_row["needs_review"],
            "definicao": definicao or "",
            "gloss_lang": gloss_lang or "",
            "exemplos": exemplos,
            "translations": translations,
            "notas": entry_row["notes"] or "",
            "raw_text": definicao or "",
        }
        items.append(item)

    blocks = []
    lookup = {}
    for idx in range(0, len(items), block_size):
        block_items = items[idx: idx + block_size]
        block_idx = idx // block_size + 1
        block_file = f"meta/{volume_id}-block-{block_idx:05d}.json"
        out_path = out_base / block_file
        out_path.write_text(json.dumps(block_items, ensure_ascii=False, indent=2), encoding="utf-8")
        blocks.append(
            {
                "start_idx": idx + 1,
                "end_idx": idx + len(block_items),
                "file": block_file,
                "count": len(block_items),
            }
        )
        for local_idx, itm in enumerate(block_items):
            lookup[itm["id"]] = {"block_file": block_file, "idx_in_block": local_idx}

    meta_obj = {
        "volume_id": volume_id,
        "title": source["name"],
        "page_min": None,
        "page_max": None,
        "blocks": blocks,
    }
    (out_base / "meta" / f"{volume_id}.json").write_text(
        json.dumps(meta_obj, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    (out_base / "lookup" / f"{volume_id}-id-to-block.json").write_text(
        json.dumps(lookup, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    (out_base / "dict" / "morfologia.json").write_text(
        json.dumps(sorted(morph_values), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (out_base / "dict" / "lemmas-first-letter.json").write_text(
        json.dumps(letter_counter, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    top_lemmas = [
        {"label": lemma, "count": cnt}
        for lemma, cnt in lemma_counter.most_common(500)
    ]
    (out_base / "dict" / "lemmas-top.json").write_text(
        json.dumps({"items": top_lemmas}, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    return {
        "volume_id": volume_id,
        "title": source["name"],
        "blocks_prefix": f"data/{volume_id}/",
        "meta_url": f"data/{volume_id}/meta/{volume_id}.json",
        "page_min": None,
        "page_max": None,
    }


def write_forms_top(out_root: Path, counter: Counter):
    if not counter:
        return
    top_items = [
        {"form": form, "count": cnt}
        for form, cnt in counter.most_common(5000)
    ]
    for vol_dir in out_root.iterdir():
        if (vol_dir / "dict").is_dir():
            (vol_dir / "dict" / "forms-top.json").write_text(
                json.dumps(top_items, ensure_ascii=False, indent=2), encoding="utf-8"
            )


def main():
    parser = argparse.ArgumentParser(description="Exporta web/public/data a partir do superdb.sqlite")
    parser.add_argument("--db", default="dicionarios/superdb.sqlite")
    parser.add_argument("--out", default="web/public/data")
    parser.add_argument("--sources", default="1,2,3,4,5,6,7,8")
    parser.add_argument("--block-size", type=int, default=500)
    parser.add_argument("--lang-priority", default="pt,en,fr,la")
    args = parser.parse_args()

    lang_priority = args.lang_priority.split(",")
    sources = parse_csv_ids(args.sources)
    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    # Evita erros "unable to open database file" ao criar temp tables
    conn.execute("PRAGMA temp_store=2")
    conn.execute("PRAGMA cache_size=100000")

    source_list = load_sources(conn, sources)
    volumes_public = []
    forms_counter = Counter()

    for src in source_list:
        print(f"Processando source {src['id']} ({src['slug']})...")
        volume_meta = export_volume(conn, src, out_root, args.block_size, lang_priority, forms_counter)
        volumes_public.append(volume_meta)

    volumes_json = out_root / "volumes.json"
    volumes_json.write_text(
        json.dumps(volumes_public, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Gerado {volumes_json}")

    write_forms_top(out_root, forms_counter)
    print("Concluído.")


if __name__ == "__main__":
    main()
