
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera dumps determinísticos (JSONL) do lexicon para diffs no Git.

- entry.jsonl: entries com lemas agregados e campos principais
- exemplo.jsonl: citações (raw + normalizado), status e confiança
"""
import argparse, json, sqlite3, os

def rows_iter(conn, sql, params=()):
    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    for r in cur:
        yield {k: v for k, v in zip(cols, r)}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out-dir", default="exports")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    conn = sqlite3.connect(args.db)

    # Entries (ordenadas por id)
    sql_entry = """
    SELECT e.id,
           e.doc_name,
           e.page_num,
           e.morfologia,
           e.definicao,
           e.notas,
           COALESCE((SELECT group_concat(forma, ' / ') FROM lemma WHERE entry_id=e.id),'') AS lemas
    FROM entry e
    ORDER BY e.id
    """
    with open(os.path.join(args.out_dir, 'entry.jsonl'), 'w', encoding='utf-8') as f:
        for row in rows_iter(conn, sql_entry):
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    # Exemplos (ordenados por id)
    sql_ex = """
    SELECT r.id AS exemplo_id,
           r.entry_id,
           r.raw_ref,
           r.texto,
           r.origem,
           COALESCE(n.parse_status,'invalido') AS parse_status,
           n.fonte_abrev,
           n.obra_abrev,
           n.loc_str,
           COALESCE(n.confidence,0.0) AS confidence
    FROM exemplo_raw r
    LEFT JOIN exemplo_norm n ON n.raw_id = r.id
    ORDER BY r.id
    """
    with open(os.path.join(args.out_dir, 'exemplo.jsonl'), 'w', encoding='utf-8') as f:
        for row in rows_iter(conn, sql_ex):
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    main()
