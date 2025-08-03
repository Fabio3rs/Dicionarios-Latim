
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CLI para consultar o lexicon SQLite com FTS5.

Novos recursos:
  --id <N>                    Busca direta por entry id.
  --with-exemplos             Lista citações do verbete (com --id ou junto do resultado FTS).
  --status {ok,parcial,invalido}
                              Filtra por parse_status das citações.
  --doc-name <str>, --page <n>
                              Filtros por origem/página (para buscas FTS de entries).
  --confidence-min <float>    Filtra citações pela confiança mínima.
  --raw                       Inclui raw_text da página nos resultados de entries.
  --export {csv,jsonl} --out <arquivo>
                              Exporta resultados.
  --count-only                Retorna apenas a contagem.

Exemplos:
  python query_lexicon.py --db lexicon.db --fts-entry "adamant*" --doc-name chunk_464 --page 464
  python query_lexicon.py --db lexicon.db --id 332 --with-exemplos --status ok --confidence-min 0.75
  python query_lexicon.py --db lexicon.db --fts-exemplo "Cic. OR Verg." --status ok --mode json --export jsonl --out res.jsonl

Observações:
  - Ordenação por relevância usando bm25() (menor = mais relevante), quando disponível.
  - Highlight via snippet().
"""

import argparse
import json
import sqlite3
import sys
from typing import Any, Dict, List, Optional

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# ----------------------- Entries (FTS e filtros) -----------------------

def build_entries_where(doc_name: Optional[str], page: Optional[int]) -> (str, list):
    clauses = []
    params = []
    if doc_name is not None:
        clauses.append("e.doc_name = ?")
        params.append(doc_name)
    if page is not None:
        clauses.append("e.page_num = ?")
        params.append(page)
    where_sql = (" AND " + " AND ".join(clauses)) if clauses else ""
    return where_sql, params

def count_entries(conn: sqlite3.Connection, query: str, doc_name: Optional[str], page: Optional[int]) -> int:
    where_sql, params = build_entries_where(doc_name, page)
    sql = f"""
    SELECT COUNT(1) AS c
    FROM entry_fts
    JOIN entry e ON e.id = entry_fts.rowid
    WHERE entry_fts MATCH ?{where_sql};
    """
    return conn.execute(sql, (query, *params)).fetchone()["c"]

def search_entries(conn: sqlite3.Connection, query: str, limit: int, hs: str, he: str,
                   doc_name: Optional[str], page: Optional[int], include_raw: bool) -> List[Dict[str, Any]]:
    where_sql, params = build_entries_where(doc_name, page)
    order = "ORDER BY bm25(entry_fts)"  # tentaremos; se fall back, será tratado
    select_raw = ", e.raw_text" if include_raw else ""
    sql = f"""
    SELECT
        e.id,
        COALESCE((SELECT group_concat(forma, ' / ')
                  FROM lemma WHERE entry_id = e.id), '') AS lemas,
        snippet(entry_fts, 1, ?, ?, '…', 12) AS snippet_def,   -- definicao
        snippet(entry_fts, 4, ?, ?, '…', 12) AS snippet_raw,   -- raw_text
        e.morfologia,
        e.definicao,
        e.notas
        {select_raw}
    FROM entry_fts
    JOIN entry e ON e.id = entry_fts.rowid
    WHERE entry_fts MATCH ?{where_sql}
    {order}
    LIMIT ?;
    """
    try:
        rows = conn.execute(sql, (hs, he, hs, he, query, *params, limit)).fetchall()
    except sqlite3.OperationalError as e:
        if "no such function: bm25" in str(e):
            sql = sql.replace(order, "")
            rows = conn.execute(sql, (hs, he, hs, he, query, *params, limit)).fetchall()
        else:
            raise
    out = []
    for r in rows:
        item = {
            "id": r["id"],
            "lemas": r["lemas"],
            "morfologia": r["morfologia"],
            "snippet": r["snippet_def"] or r["snippet_raw"],
            "definicao": r["definicao"],
            "notas": r["notas"],
        }
        if include_raw:
            item["raw_text"] = r["raw_text"]
        out.append(item)
    return out

# ----------------------- Examples (FTS e filtros) -----------------------

def build_examples_where(status: Optional[str], fonte: Optional[str], obra: Optional[str],
                         confidence_min: Optional[float]) -> (str, list):
    where = ["exemplo_fts MATCH ?"]
    params: List[Any] = []
    if fonte:
        where.append("COALESCE(exemplo_fts.fonte_abrev,'') LIKE ?")
        params.append(f"%{fonte}%")
    if obra:
        where.append("COALESCE(exemplo_fts.obra_abrev,'') LIKE ?")
        params.append(f"%{obra}%")
    if status:
        where.append("COALESCE(n.parse_status,'invalido') = ?")
        params.append(status)
    if confidence_min is not None:
        where.append("COALESCE(n.confidence,0.0) >= ?")
        params.append(confidence_min)
    return " AND ".join(where), params

def count_examples(conn: sqlite3.Connection, query: str, status: Optional[str], fonte: Optional[str],
                   obra: Optional[str], confidence_min: Optional[float]) -> int:
    where_sql, params = build_examples_where(status, fonte, obra, confidence_min)
    sql = f"""
    SELECT COUNT(1) AS c
    FROM exemplo_fts
    LEFT JOIN exemplo_norm n ON n.raw_id = exemplo_fts.rowid
    WHERE {where_sql};
    """
    return conn.execute(sql, (query, *params)).fetchone()["c"]

def search_examples(conn: sqlite3.Connection, query: str, limit: int, hs: str, he: str,
                    status: Optional[str], fonte: Optional[str], obra: Optional[str],
                    confidence_min: Optional[float]) -> List[Dict[str, Any]]:
    where_sql, params = build_examples_where(status, fonte, obra, confidence_min)
    order = "ORDER BY bm25(exemplo_fts)"
    sql = f"""
    SELECT
        v.exemplo_id,
        v.entry_id,
        v.referencia,
        COALESCE(n.parse_status,'invalido') AS status,
        snippet(exemplo_fts, 0, ?, ?, '…', 12) AS snippet_ref, -- raw_ref
        exemplo_fts.fonte_abrev,
        exemplo_fts.obra_abrev,
        exemplo_fts.loc_str,
        COALESCE(n.confidence,0.0) AS confidence
    FROM exemplo_fts
    JOIN exemplo_view v ON v.exemplo_id = exemplo_fts.rowid
    LEFT JOIN exemplo_norm n ON n.raw_id = exemplo_fts.rowid
    WHERE {where_sql}
    {order}
    LIMIT ?;
    """
    try:
        rows = conn.execute(sql, (hs, he, query, *params, limit)).fetchall()
    except sqlite3.OperationalError as e:
        if "no such function: bm25" in str(e):
            sql = sql.replace(order, "")
            rows = conn.execute(sql, (hs, he, query, *params, limit)).fetchall()
        else:
            raise
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
            "confidence": r["confidence"],
        })
    return out

# ----------------------- Entry by ID (+ exemplos) -----------------------

def get_entry_by_id(conn: sqlite3.Connection, entry_id: int, with_exemplos: bool,
                    status: Optional[str], confidence_min: Optional[float]) -> Dict[str, Any]:
    e = conn.execute("""
        SELECT e.*, COALESCE((SELECT group_concat(forma, ' / ') FROM lemma WHERE entry_id=e.id),'') AS lemas
        FROM entry e WHERE e.id = ?
    """, (entry_id,)).fetchone()
    if not e:
        return {}

    result = {
        "id": e["id"],
        "lemas": e["lemas"],
        "morfologia": e["morfologia"],
        "definicao": e["definicao"],
        "notas": e["notas"],
        "doc_name": e["doc_name"],
        "page_num": e["page_num"],
        "raw_text": e["raw_text"],
    }

    if with_exemplos:
        where = ["r.entry_id = ?"]
        params: List[Any] = [entry_id]
        if status:
            where.append("COALESCE(n.parse_status,'invalido') = ?")
            params.append(status)
        if confidence_min is not None:
            where.append("COALESCE(n.confidence,0.0) >= ?")
            params.append(confidence_min)
        where_sql = " AND ".join(where)

        ex_rows = conn.execute(f"""
            SELECT v.exemplo_id, v.referencia,
                   COALESCE(n.parse_status,'invalido') AS status,
                   COALESCE(n.confidence,0.0) AS confidence
            FROM exemplo_raw r
            JOIN exemplo_view v ON v.exemplo_id = r.id
            LEFT JOIN exemplo_norm n ON n.raw_id = r.id
            WHERE {where_sql}
            ORDER BY v.exemplo_id
        """, params).fetchall()

        result["exemplos"] = [{
            "exemplo_id": x["exemplo_id"],
            "referencia": x["referencia"],
            "status": x["status"],
            "confidence": x["confidence"],
        } for x in ex_rows]

    return result

# ----------------------- Export helpers -----------------------

def export_rows(rows: List[Dict[str, Any]], mode: str, out_path: str):
    if not rows:
        with open(out_path, "w", encoding="utf-8") as f:
            pass
        return
    if mode == "jsonl":
        with open(out_path, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    elif mode == "csv":
        import csv
        keys = sorted({k for r in rows for k in r.keys()})
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in keys})

# ----------------------- Main -----------------------

def main():
    ap = argparse.ArgumentParser(description="Consulta FTS ao lexicon SQLite.")
    ap.add_argument("--db", required=True, help="Caminho para o SQLite DB.")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--fts-entry", help="Consulta FTS nas entradas (entry_fts).")
    group.add_argument("--fts-exemplo", help="Consulta FTS nas citações (exemplo_fts).")
    group.add_argument("--id", type=int, help="Busca direta por entry id.")

    ap.add_argument("--limit", type=int, default=20, help="Limite de resultados (default 20).")
    ap.add_argument("--status", choices=["ok", "parcial", "invalido"], help="Filtrar por parse_status (exemplos).")
    ap.add_argument("--fonte", help="Filtro contém em fonte_abrev (exemplos).")
    ap.add_argument("--obra", help="Filtro contém em obra_abrev (exemplos).")
    ap.add_argument("--doc-name", help="Filtro por doc_name (entries FTS).")
    ap.add_argument("--page", type=int, help="Filtro por page_num (entries FTS).")
    ap.add_argument("--confidence-min", type=float, help="Confiança mínima (exemplos).")
    ap.add_argument("--with-exemplos", action="store_true", help="Inclui citações do verbete (com --id ou após FTS).")
    ap.add_argument("--raw", action="store_true", help="Inclui raw_text em resultados de entries.")
    ap.add_argument("--mode", choices=["plain", "json"], default="plain", help="Formato de saída.")
    ap.add_argument("--export", choices=["csv", "jsonl"], help="Exportar resultados.")
    ap.add_argument("--out", help="Arquivo de saída para export.")
    ap.add_argument("--count-only", action="store_true", help="Somente contagem.")

    ap.add_argument("--highlight-start", default="[", help="Marcador inicial do highlight/snippet.")
    ap.add_argument("--highlight-end", default="]", help="Marcador final do highlight/snippet.")
    args = ap.parse_args()

    conn = connect(args.db)

    # COUNT-ONLY shortcuts
    if args.count_only:
        if args.fts_entry:
            c = count_entries(conn, args.fts_entry, args.doc_name, args.page)
            print(c)
            return
        elif args.fts_exemplo:
            c = count_examples(conn, args.fts_exemplo, args.status, args.fonte, args.obra, args.confidence_min)
            print(c)
            return
        else:
            # --id não faz muito sentido com count-only; retorna 1 ou 0
            e = conn.execute("SELECT 1 FROM entry WHERE id = ?", (args.id,)).fetchone()
            print(1 if e else 0)
            return

    rows: List[Dict[str, Any]] = []

    if args.id is not None:
        entry = get_entry_by_id(conn, args.id, args.with_exemplos, args.confidence_min if args.with_exemplos else None,
                                args.confidence_min if args.with_exemplos else None)
        if not entry:
            print("Nenhum resultado.")
            return
        if args.mode == "json":
            print(json.dumps(entry, ensure_ascii=False, indent=2))
        else:
            print(f"[entry #{entry['id']}] {entry['lemas']}")
            if entry.get("morfologia"):
                print(f"  • morf: {entry['morfologia']}")
            if entry.get("definicao"):
                print(f"  • definicao: {entry['definicao']}")
            if entry.get("notas"):
                print(f"  • notas: {entry['notas']}")
            print(f"  • origem: {entry['doc_name']} p.{entry['page_num']}")
            if args.raw and entry.get("raw_text"):
                print(f"  • raw: {entry['raw_text'][:500]}{'…' if len(entry['raw_text'])>500 else ''}")
            if args.with_exemplos and entry.get("exemplos"):
                for ex in entry["exemplos"]:
                    print(f"    - [exemplo #{ex['exemplo_id']}] {ex['status']} (conf={ex['confidence']:.2f}) — {ex['referencia']}")
        # export for --id not implemented (single object), but we can write JSON
        if args.export and args.out:
            if args.export == "jsonl":
                with open(args.out, "w", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            elif args.export == "csv":
                # Flatten simples (sem exemplos)
                flat = {k: v for k, v in entry.items() if k != "exemplos"}
                import csv
                with open(args.out, "w", encoding="utf-8", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=list(flat.keys()))
                    w.writeheader()
                    w.writerow(flat)
        return

    if args.fts_entry:
        rows = search_entries(conn, args.fts_entry, args.limit, args.highlight_start, args.highlight_end,
                              args.doc_name, args.page, args.raw)
        # opcionalmente anexar exemplos por entry
        if args.with_exemplos and rows:
            for r in rows:
                ex = conn.execute("""
                    SELECT v.exemplo_id, v.referencia, COALESCE(n.parse_status,'invalido') AS status,
                           COALESCE(n.confidence,0.0) AS confidence
                    FROM exemplo_raw rr
                    JOIN exemplo_view v ON v.exemplo_id = rr.id
                    LEFT JOIN exemplo_norm n ON n.raw_id = rr.id
                    WHERE rr.entry_id = ?
                    ORDER BY v.exemplo_id
                """, (r["id"],)).fetchall()
                r["exemplos"] = [{
                    "exemplo_id": x["exemplo_id"],
                    "referencia": x["referencia"],
                    "status": x["status"],
                    "confidence": x["confidence"],
                } for x in ex]

    elif args.fts_exemplo:
        rows = search_examples(conn, args.fts_exemplo, args.limit, args.highlight_start, args.highlight_end,
                               status=args.status, fonte=args.fonte, obra=args.obra, confidence_min=args.confidence_min)

    # Export/print
    if args.export and args.out:
        export_rows(rows, args.export, args.out)

    if args.mode == "json":
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        if not rows:
            print("Nenhum resultado.")
            return
        if args.fts_entry:
            for r in rows:
                header = f"[entry #{r['id']}] {r['lemas']}"
                print(header)
                if r.get('morfologia'):
                    print(f"  • morf: {r['morfologia']}")
                if r.get('snippet'):
                    print(f"  • trecho: {r['snippet']}")
                if r.get('definicao'):
                    print(f"  • definicao: {r['definicao']}")
                if r.get('notas'):
                    print(f"  • notas: {r['notas']}")
                if args.raw and r.get('raw_text'):
                    print(f"  • raw: {r['raw_text'][:500]}{'…' if len(r['raw_text'])>500 else ''}")
                if args.with_exemplos and r.get("exemplos"):
                    for ex in r["exemplos"]:
                        print(f"    - [exemplo #{ex['exemplo_id']}] {ex['status']} (conf={ex['confidence']:.2f}) — {ex['referencia']}")
                print("")
        else:
            for r in rows:
                header = f"[exemplo #{r['exemplo_id']}] entry={r['entry_id']} status={r['status']} (conf={r['confidence']:.2f})"
                print(header)
                print(f"  • ref: {r['referencia']}")
                if r.get('snippet'):
                    print(f"  • trecho: {r['snippet']}")
                extra = []
                if r.get('fonte_abrev'):
                    extra.append(f"fonte={r['fonte_abrev']}")
                if r.get('obra_abrev'):
                    extra.append(f"obra={r['obra_abrev']}")
                if r.get('loc_str'):
                    extra.append(f"loc={r['loc_str']}")
                if extra:
                    print("  • " + "; ".join(extra))
                print("")

if __name__ == "__main__":
    main()
