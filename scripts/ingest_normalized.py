#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ingest normalized_results.json into SQLite per schema_normalizado.sql.
- Creates DB if it doesn't exist and applies schema.
- Inserts entries, lemmas, raw examples.
- Heuristically normalizes example references into exemplo_norm with parse_status and confidence.
- Flags suspicious items (parse_status != 'ok' or confidence < 0.75).

Usage:
  python ingest_normalized.py --json /path/normalized_results.json --db /path/lexicon.db --schema /path/schema_normalizado.sql
"""
import argparse
import json
import os
import re
import sqlite3

# Try to import abbreviations map if available (dump_example.py)
ABBREV = {}
try:
    from dump_example import abreviaturas as _AB
    # Flatten to sets for quick membership tests
    AUTHOR_ABBREVS = set(_AB.keys())
    WORK_ABBREVS = set()
    for k, v in _AB.items():
        for o in v.get("obras", []):
            # Accept left part before '=' and strip dots/spaces
            left = o.split("=")[0].strip()
            WORK_ABBREVS.add(left)
    ABBREV = {"authors": AUTHOR_ABBREVS, "works": WORK_ABBREVS}
except Exception:
    ABBREV = {"authors": set(), "works": set()}


# ---------- Helpers ----------

WS_RE = re.compile(r"\s+", re.UNICODE)
TRAIL_PUNCT_RE = re.compile(r"[;,\.\)\]]+$")
BAD_CHARS_RE = re.compile(r"[^\w\.\,\-\s\(\)ÁÉÍÓÚÂÊÎÔÛÃÕÀÇáéíóúâêîôûãõàç]")

def norm_space(s: str) -> str:
    return WS_RE.sub(" ", s or "").strip()

def split_ref(s: str):
    """
    Try to split 'Cic. Div. 1, 23' => ('Cic.', 'Div.', '1, 23')
    Also accept cases like 'Verg. En. 6, 552' etc.
    Return (fonte_abrev, obra_abrev, loc_str, notes, issues, parse_status, confidence)
    """
    issues = []
    notes = []

    s = (s or "").strip()
    if not s:
        issues.append("vazio")
        return None, None, None, notes, issues, "invalido", 0.0

    # Cleanups
    s = s.replace("—", "-").replace("–", "-").replace("•", "")
    s = s.replace("·", ".").replace("..", ".")
    s = s.replace(" ,", ",").replace(" , ", ", ").replace(" .", ".")
    s = s.replace("cit. LS", "").replace("(cit. LS)", "")
    s = TRAIL_PUNCT_RE.sub("", s)
    s = norm_space(s)

    # Basic sanity
    if BAD_CHARS_RE.search(s):
        issues.append("caracteres_estranhos")

    # Common harmonizations (heuristics)
    s = s.replace("1 Ped.", "1 Pet.")  # heuristic
    s = s.replace("App. M.", "Apul. M.")  # heuristic seen in dumps

    # Capture patterns
    m = re.match(r"^([A-ZÁÉÍÓÚÂÊÎÔÛÃÕÀÇ][A-Za-zÁÉÍÓÚÂÊÎÔÛÃÕÀÇ\. ]+?)\s+([A-Za-zÁÉÍÓÚÂÊÎÔÛÃÕÀÇ][A-Za-zÁÉÍÓÚÂÊÎÔÛÃÕÀÇ\. ]+?)\s+(.+)$", s)
    fonte_abrev = obra_abrev = loc_str = None
    if m:
        fonte_abrev = norm_space(m.group(1))
        obra_abrev  = norm_space(m.group(2))
        loc_str     = norm_space(m.group(3))
    else:
        m2 = re.match(r"^([A-ZÁÉÍÓÚÂÊÎÔÛÃÕÀÇ][A-Za-zÁÉÍÓÚÂÊÎÔÛÃÕÀÇ\. ]+?)\s+(.+)$", s)
        if m2:
            fonte_abrev = norm_space(m2.group(1))
            obra_abrev  = None
            loc_str     = norm_space(m2.group(2))
            issues.append("obra_ausente")
        else:
            issues.append("nao_casou")

    # Evaluate numeric location quality
    if loc_str:
        if not re.search(r"\d", loc_str):
            issues.append("sem_numeros_em_loc")
        if not re.search(r"(\d+\s*(,\s*\d+)*|p\.\s*\d+|l\.\s*\d+|pr\.|pref\.)", loc_str, flags=re.IGNORECASE):
            notes.append("loc_formato_atipico")

    # Confidence & parse_status
    confidence = 0.9
    parse_status = "ok"

    # Validate against abbrev map if available
    if ABBREV["authors"]:
        a_token = fonte_abrev.split()[0] if fonte_abrev else ""
        if a_token and a_token.rstrip(".") not in {x.rstrip(".") for x in ABBREV["authors"]}:
            notes.append("autor_desconhecido")
            confidence -= 0.15
    if obra_abrev and ABBREV["works"]:
        w_token = obra_abrev.split()[0]
        if w_token and w_token.rstrip(".") not in {x.rstrip(".") for x in ABBREV["works"]}:
            notes.append("obra_desconhecida")
            confidence -= 0.1

    if "nao_casou" in issues or "vazio" in issues:
        parse_status = "invalido"
        confidence = min(confidence, 0.2)
    elif "obra_ausente" in issues:
        parse_status = "parcial"
        confidence = min(confidence, 0.6)

    if "caracteres_estranhos" in issues:
        confidence -= 0.2
    if "sem_numeros_em_loc" in issues:
        parse_status = "parcial"
        confidence = min(confidence, 0.4)

    confidence = max(0.0, min(1.0, confidence))

    return fonte_abrev, obra_abrev, loc_str, notes, issues, parse_status, confidence


def ensure_schema(conn, schema_path):
    with open(schema_path, "r", encoding="utf-8") as f:
        sql = f.read()
    conn.executescript(sql)


def upsert_entry(conn, doc_name, page_num, raw_text, morfologia, definicao, notas):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO entry (doc_name, page_num, raw_text, morfologia, definicao, notas)
        VALUES (?, ?, ?, ?, ?, ?)
        """.strip(),
        (doc_name, page_num, raw_text, morfologia, definicao, notas),
    )
    return cur.lastrowid


def insert_lemma(conn, entry_id, forma):
    conn.execute(
        "INSERT INTO lemma (entry_id, forma) VALUES (?, ?)",
        (entry_id, forma),
    )


def insert_exemplo(conn, entry_id, raw_ref, origem="normalized_results"):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO exemplo_raw (entry_id, raw_ref, origem) VALUES (?, ?, ?)",
        (entry_id, raw_ref, origem),
    )
    raw_id = cur.lastrowid

    fonte_abrev, obra_abrev, loc_str, notes, issues, parse_status, confidence = split_ref(raw_ref)
    err_bits = []
    if issues:
        err_bits.append(f"issues={','.join(issues)}")
    if notes:
        err_bits.append(f"notes={','.join(notes)}")
    err_msg = "; ".join(err_bits) if err_bits else None

    conn.execute(
        """
        INSERT INTO exemplo_norm
          (raw_id, parse_status, err_msg, fonte_abrev, obra_abrev, loc_str, confidence)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """.strip(),
        (raw_id, parse_status, err_msg, fonte_abrev, obra_abrev, loc_str, confidence),
    )

    return raw_id


def populate_entry_fts(conn, entry_id):
    conn.execute("INSERT INTO entry_fts(entry_fts, rowid) VALUES('delete', ?)", (entry_id,))
    conn.execute(
        """
        INSERT INTO entry_fts(rowid, lemas, definicao, morfologia, notas, raw_text)
        SELECT rowid, lemas, definicao, morfologia, notas, raw_text
        FROM entry_fts_source WHERE rowid = ?
        """.strip(),
        (entry_id,),
    )


def populate_exemplo_fts(conn, raw_id):
    conn.execute("INSERT INTO exemplo_fts(exemplo_fts, rowid) VALUES('delete', ?)", (raw_id,))
    conn.execute(
        """
        INSERT INTO exemplo_fts(rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str)
        SELECT rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str
        FROM exemplo_fts_source WHERE rowid = ?
        """.strip(),
        (raw_id,),
    )


def main():
    parser = argparse.ArgumentParser(description="Ingest normalized_results.json into SQLite.")
    parser.add_argument("--json", required=True, help="Path to normalized_results.json")
    parser.add_argument("--db", default="lexicon.db", help="SQLite DB path (created if not exists)")
    parser.add_argument("--schema", default="schema_normalizado.sql", help="Schema SQL path")
    parser.add_argument("--batch-fts", action="store_true", help="Defer FTS population (faster for large loads)")
    args = parser.parse_args()

    if not os.path.exists(args.json):
        raise FileNotFoundError(f"JSON not found: {args.json}")
    if not os.path.exists(args.schema):
        raise FileNotFoundError(f"Schema not found: {args.schema}")

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys = ON;")
    ensure_schema(conn, args.schema)

    with open(args.json, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Bulk load
    entry_count = 0
    lemma_count = 0
    raw_ex_count = 0
    norm_ex_ok = norm_ex_parcial = norm_ex_invalido = 0

    for entry in data:
        doc_name      = entry.get("doc_name")
        page_num      = entry.get("page_num")
        raw_text      = entry.get("raw_text")
        norm_items    = entry.get("normalized_text") or []

        for item in norm_items:
            morfologia = item.get("morfologia")
            definicao  = item.get("definicao")
            notas      = item.get("notas")
            lemmas     = item.get("lemas") or []
            exemplos   = item.get("exemplos") or []

            e_id = upsert_entry(conn, doc_name, page_num, raw_text, morfologia, definicao, notas)
            entry_count += 1

            for lem in lemmas:
                if lem:
                    insert_lemma(conn, e_id, lem.strip())
                    lemma_count += 1

            for ex in exemplos:
                if ex is None:
                    continue
                raw_id = insert_exemplo(conn, e_id, str(ex).strip(), origem="normalized_results")
                raw_ex_count += 1

                st = conn.execute("SELECT parse_status FROM exemplo_norm WHERE raw_id = ?", (raw_id,)).fetchone()[0]
                if st == "ok":
                    norm_ex_ok += 1
                elif st == "parcial":
                    norm_ex_parcial += 1
                else:
                    norm_ex_invalido += 1

            if not args.batch_fts:
                populate_entry_fts(conn, e_id)

        conn.commit()

    if args.batch_fts:
        conn.execute("DELETE FROM entry_fts;")
        conn.execute("""
            INSERT INTO entry_fts(rowid, lemas, definicao, morfologia, notas, raw_text)
            SELECT rowid, lemas, definicao, morfologia, notas, raw_text FROM entry_fts_source;
        """.strip())
        conn.execute("DELETE FROM exemplo_fts;")
        conn.execute("""
            INSERT INTO exemplo_fts(rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str)
            SELECT rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str FROM exemplo_fts_source;
        """.strip())
        conn.commit()

    # Summary
    summary = {
        "entries_inserted": entry_count,
        "lemmas_inserted": lemma_count,
        "raw_examples_inserted": raw_ex_count,
        "normalized_ok": norm_ex_ok,
        "normalized_parcial": norm_ex_parcial,
        "normalized_invalido": norm_ex_invalido,
        "db_path": os.path.abspath(args.db)
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    conn.close()


if __name__ == "__main__":
    main()
