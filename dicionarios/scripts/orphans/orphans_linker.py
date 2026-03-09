#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
orphans_linker.py
Liga órfãos a entradas do índice FAISS (LS/Gaffiot/Retificado/Lat→Deu) usando
a definição do retificado_v2 quando necessário.

Requisitos:
  - Python 3.9+
  - pip install faiss-cpu numpy tqdm requests
  - Ollama rodando (http://localhost:11434) com modelo de embeddings
  - O módulo local faiss_ollama_multi.py (query_index) acessível no PYTHONPATH

Uso (exemplo):
  python3 orphans_linker.py \
      --orphans orphans_v2.tsv \
      --retificado retificado_v2.db \
      --index index_mix/index.faiss \
      --meta  index_mix/meta.jsonl \
      --out   matches.tsv \
      --k 10 \
      --model nomic-embed-text
"""
from __future__ import annotations
import argparse
import csv
import json
import os
import sqlite3
import unicodedata
from typing import Dict, List, Optional

# Import util da sua base
from faiss_ollama_multi import query_index  # type: ignore

# ------------------------------------------------------------
# Utilidades
# ------------------------------------------------------------

def strip_accents_lower(s: str) -> str:
    """Minúsculas sem diacríticos (sem colapsar u/v, i/j — padrão retificado v2)."""
    if not s:
        return s
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()

def best_def_from_retificado(conn: sqlite3.Connection, lemma: str, lemma_norm: Optional[str]) -> Optional[str]:
    """
    Busca definição no retificado_v2 por ordem de preferência:
      1) match exato por lemma_sort (normalizado simples)
      2) match exato por lemma (tal qual)
      3) fallback: lemma_sort LIKE prefixo (cauteloso)
    Retorna 'definicao' ou None.
    """
    if not lemma and not lemma_norm:
        return None

    # 1) lemma_sort
    cand_norm = strip_accents_lower(lemma_norm or lemma or "")
    if cand_norm:
        row = conn.execute(
            "SELECT definicao FROM entry "
            "WHERE lemma_sort = ? AND definicao <> '' "
            "ORDER BY needs_review ASC, morph_out_of_vocab ASC LIMIT 1",
            (cand_norm,)
        ).fetchone()
        if row and row[0]:
            return row[0]

    # 2) lemma literal
    if lemma:
        row = conn.execute(
            "SELECT definicao FROM entry "
            "WHERE lemma = ? AND definicao <> '' "
            "ORDER BY needs_review ASC, morph_out_of_vocab ASC LIMIT 1",
            (lemma,)
        ).fetchone()
        if row and row[0]:
            return row[0]

    # 3) LIKE por prefixo normalizado
    if cand_norm and len(cand_norm) >= 3:
        row = conn.execute(
            "SELECT definicao FROM entry "
            "WHERE lemma_sort LIKE ? AND definicao <> '' "
            "ORDER BY needs_review ASC, morph_out_of_vocab ASC LIMIT 1",
            (cand_norm + "%",)
        ).fetchone()
        if row and row[0]:
            return row[0]

    return None

def make_query_text(lemma: str, definicao_pt: str, morph: Optional[str] = None) -> str:
    """
    Texto curto e informativo para embedding de consulta.
    Priorizamos PT, pois o índice novo tem PT (LS/retificado) e FR/DE como fallback.
    """
    parts = []
    if definicao_pt:
        parts.append(f"[pt] {definicao_pt}")
    if lemma:
        parts.append(f"Lema: {lemma}")
    if morph:
        parts.append(f"Morf.: {morph}")
    return "\n".join(parts)

# ------------------------------------------------------------
# I/O
# ------------------------------------------------------------

def read_orphans_tsv(path: str) -> List[Dict[str, str]]:
    """
    Lê TSV com cabeçalho. Colunas toleradas:
      id, lemma, lemma_norm, gloss, definicao, morph
    (demais serão ignoradas)
    """
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        # Assume TSV; ainda assim detecta delimitador se não for \t
        sample = f.read(4096)
        f.seek(0)
        delim = "\t"
        try:
            sniff = csv.Sniffer().sniff(sample)
            if sniff.delimiter in (",", ";", "|", "\t"):
                delim = sniff.delimiter
        except Exception:
            pass
        rdr = csv.DictReader(f, delimiter=delim)
        for r in rdr:
            rows.append({(k or "").strip(): (v or "").strip() for k, v in r.items()})
    return rows

def write_output_tsv(path: str, rows: List[Dict[str, str]]) -> None:
    if not rows:
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter="\t")
            w.writerow([
                "src_id","lemma","lemma_norm","query_def",
                "hit_rank","score","hit_source","hit_lemma",
                "hit_id","hit_entry_id","hit_sense_id","hit_meta_json"
            ])
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        wr = csv.DictWriter(f, delimiter="\t", fieldnames=fieldnames)
        wr.writeheader()
        for r in rows:
            wr.writerow(r)

# ------------------------------------------------------------
# Pipeline
# ------------------------------------------------------------

def run(
    orphans_path: str,
    retificado_db: str,
    index_path: str,
    meta_path: str,
    out_path: str,
    model: str,
    k: int
) -> None:

    # Abre retificado_v2
    conn = sqlite3.connect(retificado_db)
    conn.row_factory = sqlite3.Row

    # Lê órfãos
    orphans = read_orphans_tsv(orphans_path)

    out_rows: List[Dict[str, str]] = []

    for r in orphans:
        src_id      = r.get("id","")
        lemma       = r.get("lemma","")
        lemma_norm  = r.get("lemma_norm","")
        gloss_in    = r.get("gloss","") or r.get("definicao","")
        morph_in    = r.get("morph","")

        # Se não veio gloss/definição, tenta retificado v2
        definicao = gloss_in or best_def_from_retificado(conn, lemma, lemma_norm) or ""

        if not definicao:
            # Sem contexto para consultar: registra sem hits
            out_rows.append({
                "src_id": src_id,
                "lemma": lemma,
                "lemma_norm": lemma_norm,
                "query_def": "",
                "hit_rank": "",
                "score": "",
                "hit_source": "",
                "hit_lemma": "",
                "hit_id": "",
                "hit_entry_id": "",
                "hit_sense_id": "",
                "hit_meta_json": ""
            })
            continue

        qtext = make_query_text(lemma, definicao, morph=morph_in)

        # Consulta FAISS (retorna dicts com chaves genéricas)
        hits = query_index(
            index_path=index_path,
            meta_path=meta_path,
            text=qtext,
            model=model,
            k=k
        ) or []

        if not hits:
            out_rows.append({
                "src_id": src_id,
                "lemma": lemma,
                "lemma_norm": lemma_norm,
                "query_def": definicao,
                "hit_rank": "",
                "score": "",
                "hit_source": "",
                "hit_lemma": "",
                "hit_id": "",
                "hit_entry_id": "",
                "hit_sense_id": "",
                "hit_meta_json": ""
            })
            continue

        # Uma linha por hit (campos genéricos + fallback)
        for rank, h in enumerate(hits, start=1):
            # score pode vir como "_score" (formato padrão do seu faiss2.py)
            score = (
                h.get("score")
                if isinstance(h.get("score"), (int, float))
                else h.get("_score")
            )
            # IDs genéricos
            hit_entry_id = (
                h.get("entry_id")
                or h.get("ls_entry_id")
                or h.get("gaffiot_entry_id")
                or ""
            )
            hit_sense_id = h.get("sense_id") or h.get("gaffiot_sense_id") or ""

            out_rows.append({
                "src_id": src_id,
                "lemma": lemma,
                "lemma_norm": lemma_norm,
                "query_def": definicao,
                "hit_rank": str(rank),
                "score": f"{(score or 0.0):.6f}",
                "hit_source": str(h.get("source","")),
                "hit_lemma": str(h.get("lemma","")),
                "hit_id": str(h.get("id","")),
                "hit_entry_id": str(hit_entry_id),
                "hit_sense_id": str(hit_sense_id),
                # Útil para auditoria/replay: guarda o dict do hit (sem explode)
                "hit_meta_json": json.dumps(
                    {k: v for k, v in h.items() if k not in {"vector"}},
                    ensure_ascii=False
                ),
            })

    conn.close()
    write_output_tsv(out_path, out_rows)

# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Resolve órfãos via retificado_v2 + FAISS (índice unificado)")
    ap.add_argument("--orphans", required=True, help="TSV de órfãos (com cabeçalho)")
    ap.add_argument("--retificado", required=True, help="retificado_v2.db (SQLite)")
    ap.add_argument("--index", required=True, help="index.faiss (índice unificado)")
    ap.add_argument("--meta", required=True, help="meta.jsonl alinhado ao índice")
    ap.add_argument("--out", required=True, help="TSV de saída com matches")
    ap.add_argument("--model", default="nomic-embed-text", help="modelo do Ollama para embeddings")
    ap.add_argument("--k", type=int, default=10, help="top-K por órfão")

    args = ap.parse_args()
    run(
        orphans_path=args.orphans,
        retificado_db=args.retificado,
        index_path=args.index,
        meta_path=args.meta,
        out_path=args.out,
        model=args.model,
        k=args.k
    )

if __name__ == "__main__":
    main()
