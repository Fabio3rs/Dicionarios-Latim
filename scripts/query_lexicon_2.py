
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CLI para consultar o lexicon SQLite com FTS5.

Requisitos do banco:
  - Tabelas/FTS conforme schema_normalizado.sql
  - FTS5 disponível no SQLite (>= 3.9) e função auxiliar bm25()

Exemplos de uso:
  python query_lexicon.py --db lexicon.db --fts-entry "adamant*"
  python query_lexicon.py --db lexicon.db --fts-exemplo "Cic. OR Verg." --status ok --limit 10
  python query_lexicon.py --db lexicon.db --fts-exemplo "Met. NEAR/5 9" --obra Met. --mode json

Observações:
  - Ordenação por relevância usando bm25() (menor = mais relevante).
  - Para highlight/snippet usa colunas do FTS (entry_fts: definicao/raw_text; exemplo_fts: raw_ref).
"""

import argparse
import json
import sqlite3
import sys
from typing import Any, Dict, List

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Garantir FKs e performance razoável para leitura
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode;")
    return conn

def search_entries(conn: sqlite3.Connection, query: str, limit: int, hs: str, he: str) -> List[Dict[str, Any]]:
    sql = '''
    SELECT
        e.id,
        COALESCE((SELECT group_concat(forma, ' / ')
                  FROM lemma WHERE entry_id = e.id), '') AS lemas,
        snippet(entry_fts, 1, ?, ?, '…', 12) AS snippet_def,   -- 1=definicao
        snippet(entry_fts, 4, ?, ?, '…', 12) AS snippet_raw,   -- 4=raw_text
        e.morfologia,
        e.definicao,
        e.notas
    FROM entry_fts
    JOIN entry e ON e.id = entry_fts.rowid
    WHERE entry_fts MATCH ?
    ORDER BY bm25(entry_fts)
    LIMIT ?;
    '''
    rows = conn.execute(sql, (hs, he, hs, he, query, limit)).fetchall()
    out = []
    for r in rows:
        out.append({
            "id": r["id"],
            "lemas": r["lemas"],
            "morfologia": r["morfologia"],
            "snippet": r["snippet_def"] or r["snippet_raw"],
            "definicao": r["definicao"],
            "notas": r["notas"],
        })
    return out

def search_examples(conn: sqlite3.Connection, query: str, limit: int, hs: str, he: str,
                    status: str = None, fonte: str = None, obra: str = None) -> List[Dict[str, Any]]:
    where = ["exemplo_fts MATCH ?"]
    params = [query]

    # Filtros: usamos as colunas do FTS (fonte_abrev/obra_abrev) e o parse_status da exemplo_norm
    if fonte:
        where.append("COALESCE(exemplo_fts.fonte_abrev,'') LIKE ?")
        params.append(f"%{fonte}%")
    if obra:
        where.append("COALESCE(exemplo_fts.obra_abrev,'') LIKE ?")
        params.append(f"%{obra}%")
    if status:
        where.append("COALESCE(n.parse_status,'invalido') = ?")
        params.append(status)

    where_sql = " AND ".join(where)

    sql = f'''
    SELECT
        v.exemplo_id,
        v.entry_id,
        v.referencia,
        COALESCE(n.parse_status,'invalido') AS status,
        snippet(exemplo_fts, 0, ?, ?, '…', 12) AS snippet_ref, -- 0=raw_ref
        exemplo_fts.fonte_abrev,
        exemplo_fts.obra_abrev,
        exemplo_fts.loc_str
    FROM exemplo_fts
    JOIN exemplo_view v ON v.exemplo_id = exemplo_fts.rowid
    LEFT JOIN exemplo_norm n ON n.raw_id = exemplo_fts.rowid
    WHERE {where_sql}
    ORDER BY bm25(exemplo_fts)
    LIMIT ?;
    '''
    rows = conn.execute(sql, (hs, he, *params, limit)).fetchall()
    out = []
    for r in rows:
        out.append({
            "exemplo_id": r["exemplo_id"],
            "entry_id": r["entry_id"],
            "referencia": r["referencia"],
            "status": r["status"],
            "snippet": r["snippet_ref"],
            "fonte_abrev": r["fonte_abrev"],
            "obra_abrev": r["obra_abrev"],
            "loc_str": r["loc_str"],
        })
    return out

def main():
    ap = argparse.ArgumentParser(description="Consulta FTS ao lexicon SQLite.")
    ap.add_argument("--db", required=True, help="Caminho para o SQLite DB.")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--fts-entry", help="Consulta FTS nas entradas (entry_fts).")
    group.add_argument("--fts-exemplo", help="Consulta FTS nas citações (exemplo_fts).")
    ap.add_argument("--limit", type=int, default=20, help="Limite de resultados (default 20).")
    ap.add_argument("--status", choices=["ok", "parcial", "invalido"], help="Filtrar por parse_status (exemplos).")
    ap.add_argument("--fonte", help="Filtro contém em fonte_abrev (exemplos).")
    ap.add_argument("--obra", help="Filtro contém em obra_abrev (exemplos).")
    ap.add_argument("--mode", choices=["plain", "json"], default="plain", help="Formato de saída.")
    ap.add_argument("--highlight-start", default="[", help="Marcador inicial do highlight/snippet.")
    ap.add_argument("--highlight-end", default="]", help="Marcador final do highlight/snippet.")
    args = ap.parse_args()

    conn = connect(args.db)

    try:
        if args.fts_entry:
            rows = search_entries(conn, args.fts_entry, args.limit, args.highlight_start, args.highlight_end)
        else:
            rows = search_examples(conn, args.fts_exemplo, args.limit, args.highlight_start, args.highlight_end,
                                   status=args.status, fonte=args.fonte, obra=args.obra)
    except sqlite3.OperationalError as e:
        # Dica comum: certificar FTS5 e bm25 disponíveis
        msg = str(e)
        sys.stderr.write("ERRO SQLite: %s\n" % msg)
        if "no such function: bm25" in msg:
            sys.stderr.write("Dica: sua build do SQLite pode não ter a função auxiliar bm25(). "
                             "Tente remover 'ORDER BY bm25(...)' ou atualizar para uma versão com FTS5 completo.\n")
        sys.exit(1)

    if args.mode == "json":
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        if not rows:
            print("Nenhum resultado.")
            return
        # Impressão amigável
        if args.fts_entry:
            for r in rows:
                header = f"[entry #{r['id']}] {r['lemas']}"
                print(header)
                if r['morfologia']:
                    print(f"  • morf: {r['morfologia']}")
                if r['snippet']:
                    print(f"  • trecho: {r['snippet']}")
                if r['definicao']:
                    print(f"  • definicao: {r['definicao']}")
                if r['notas']:
                    print(f"  • notas: {r['notas']}")
                print("")
        else:
            for r in rows:
                header = f"[exemplo #{r['exemplo_id']}] entry={r['entry_id']} status={r['status']}"
                print(header)
                print(f"  • ref: {r['referencia']}")
                if r['snippet']:
                    print(f"  • trecho: {r['snippet']}")
                extra = []
                if r['fonte_abrev']:
                    extra.append(f"fonte={r['fonte_abrev']}")
                if r['obra_abrev']:
                    extra.append(f"obra={r['obra_abrev']}")
                if r['loc_str']:
                    extra.append(f"loc={r['loc_str']}")
                if extra:
                    print("  • " + "; ".join(extra))
                print("")

if __name__ == "__main__":
    main()
