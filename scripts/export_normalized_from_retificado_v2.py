#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exporta o retificado_v2.db para um JSON no formato semelhante ao normalized_results.json.

Saída padrão: resultados/normalized_results_v2.json

Estrutura por entrada:
  {
    "doc_name": "chunk_123",
    "page_num": 123,
    "raw_text": "",
    "extracted_text": "",
    "normalized_text": [
      {
        "lemas": [...],
        "morfologia": "...",          # morph_render
        "morph_extra": [...],          # lista a partir de morph_extra (JSON)
        "definicao": "...",
        "exemplos": [],
        "notas": "...",
        "conf": "...",
        "needs_review": 0/1,
        "redirect_only": 0/1,
        "morph_out_of_vocab": 0/1
      },
      ...
    ]
  }

Observações:
- `page_num` é inferido do id (formato chunk_<n>:<page>:<idx>); se não parsear, fica None.
- `lemas` inclui o lemma canônico e todas as formas de entry_form.
- Campos raw_text / extracted_text ficam vazios (não existem no DB).
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Any


def parse_id(entry_id: str):
    """
    Espera ids tipo 'chunk_13:13:0'. Retorna (doc_name, page_num:int|None).
    """
    parts = entry_id.split(":")
    doc_name = parts[0] if parts else entry_id
    page_num = None
    if len(parts) >= 2 and parts[1].isdigit():
        page_num = int(parts[1])
    return doc_name, page_num


def load_forms(conn) -> Dict[int, List[str]]:
    cur = conn.cursor()
    cur.execute("SELECT entry_id, form FROM entry_form")
    forms = defaultdict(list)
    for entry_id, form in cur.fetchall():
        forms[entry_id].append(form)
    return forms


def export(db_path: Path, out_path: Path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    forms_map = load_forms(conn)

    cur.execute(
        """
        SELECT entry_id, id, lemma, morph_render, definicao, notas, conf,
               needs_review, redirect_only, morph_out_of_vocab, morph_extra,
               raw_json
        FROM entry
        ORDER BY entry_id
        """
    )

    grouped: Dict[tuple, Dict[str, Any]] = {}
    for row in cur.fetchall():
        doc_name, page_num = parse_id(row["id"])
        key = (doc_name, page_num)
        if key not in grouped:
            grouped[key] = {
                "doc_name": doc_name,
                "page_num": page_num,
                "raw_text": "",
                "extracted_text": "",
                "normalized_text": [],
            }

        lemas = [row["lemma"]] + forms_map.get(row["entry_id"], [])
        # morph_extra é armazenado como JSON string
        try:
            morph_extra = json.loads(row["morph_extra"] or "[]")
        except Exception:
            morph_extra = []

        grouped[key]["normalized_text"].append(
            {
                "lemas": lemas,
                "morfologia": row["morph_render"],
                "morph_extra": morph_extra,
                "definicao": row["definicao"] or "",
                "exemplos": [],
                "notas": row["notas"] or "",
                "conf": row["conf"],
                "needs_review": row["needs_review"],
                "redirect_only": row["redirect_only"],
                "morph_out_of_vocab": row["morph_out_of_vocab"],
                # preserva JSON bruto da entrada original (string)
                "raw_json": row["raw_json"] or "",
            }
        )

    conn.close()

    # Ordenar por doc_name, page_num
    ordered = sorted(
        grouped.values(), key=lambda x: (x["doc_name"], x["page_num"] or 0)
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(ordered, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Exportado {len(ordered)} grupos para {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Exporta retificado_v2.db para normalized_results_v2.json"
    )
    parser.add_argument(
        "--db",
        default="dicionarios/retificado_v2.db",
        help="Caminho do SQLite de origem (default: dicionarios/retificado_v2.db)",
    )
    parser.add_argument(
        "--out",
        default="resultados/normalized_results_v2.json",
        help="Caminho do JSON de saída (default: resultados/normalized_results_v2.json)",
    )
    args = parser.parse_args()
    export(Path(args.db), Path(args.out))


if __name__ == "__main__":
    main()
