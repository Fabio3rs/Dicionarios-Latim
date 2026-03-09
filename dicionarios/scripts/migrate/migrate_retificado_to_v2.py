#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migrate_retificado_to_v2.py
---------------------------
Migra o retificado.db legado (tabela `entries`) p/ um esquema moderno alinhado ao gaffiot/ls:
  - entry / entry_form / sense / source / entry_fts (+ view 'entries' p/ compatibilidade)
  - parse do campo 'morfologia' em campos atômicos + morph_render padronizado
  - normalização de lema p/ 'lemma_sort' (unaccent+lower+j→i,v→u, espaços)

Uso:
  # prévia em CSV (sem tocar no DB de saída)
  python3 migrate_retificado_to_v2.py --in retificado.db --out retificado_v2.db --dry-run preview.csv

  # aplicar (cria o novo arquivo .db e insere dados)
  python3 migrate_retificado_to_v2.py --in retificado.db --out retificado_v2.db --apply
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import unicodedata
from typing import Optional, Tuple, List

# --------- normalização de texto/lemma ---------
def unaccent_lower(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.casefold()

def norm_lemma(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    t = unaccent_lower(s.strip())
    # t = t.replace("j", "i").replace("v", "u")
    t = re.sub(r"\s+", " ", t)
    return t

# --------- parsing de 'morfologia' legado ---------
POS_MAP = {
    "s.": "NOUN",
    "v.": "VERB",
    "adj.": "ADJ",
    "adv.": "ADV",
    "prep": "PREP",
    "conj": "CONJ",
    "interj": "INTERJ",
}

VERB_CLASSES = [
    "v. 1ª (-āre)",
    "v. 2ª (-ēre)",
    "v. 3ª (-ere)",
    "v. 3ª -iō (-ere)",
    "v. 4ª (-īre)",
]

ADJ_CLASSES = [
    "adj. (1/2) -a, -um",
    "adj. (3) 3T",
    "adj. (3) 2T",
    "adj. (3) 1T",
]

EXTRA_MARKERS = [
    "tr.", "intr.", "dep.", "impers.", "part. pres.", "part. perf. pass.", "sup."
]

def parse_morfologia(m: Optional[str]):
    """
    Retorna: (pos, gender, decl_class, verb_class, adj_class, morph_extra_json, indeclinable)
    + também retorna um morph_render (se não existir, reconstrói com base nos campos)
    """
    if not m:
        return (None, None, None, None, None, "[]", 0, None)

    raw = m
    t = raw.strip().lower()
    indecl = 1 if (" indecl" in t or " indeclin" in t or "indecl" in t) else 0

    # POS
    pos = None
    for k, v in POS_MAP.items():
        if t.startswith(k) or f" {k}" in t:
            pos = v
            break

    # gender
    gender = None
    if "masc." in t:
        gender = "m"
    elif "fem." in t:
        gender = "f"
    elif "neut." in t or "neutr." in t:
        gender = "n"

    # declinação (captura rótulo completo tipo "2ª (-i)")
    decl_class = None
    m_decl = re.search(r"(\dª)\s*\(-[a-z]+\)", raw, flags=re.IGNORECASE)
    if m_decl:
        # usamos o rótulo como está (ex.: "2ª (-i)")
        decl_class = m_decl.group(0)

    # verb/adj classes
    verb_class = None
    adj_class = None
    for lab in VERB_CLASSES:
        if lab.lower() in t:
            verb_class = lab
            break
    for lab in ADJ_CLASSES:
        if lab.lower() in t:
            adj_class = lab
            break

    # extras
    extras = []
    for kw in EXTRA_MARKERS:
        if kw in raw:
            extras.append(kw)
    morph_extra = json.dumps(extras, ensure_ascii=False)

    # morph_render: se já existia algo legível, mantemos; caso contrário renderizamos mínimo
    morph_render = raw

    return (pos, gender, decl_class, verb_class, adj_class, morph_extra, indecl, morph_render)


def render_morph(pos, gender, decl_class, verb_class, adj_class, indecl) -> Optional[str]:
    """
    Gera um rótulo canônico quando o antigo não está disponível/é vazio.
    Mantém o padrão do projeto de forma simples.
    """
    if not pos:
        return None

    if pos == "NOUN":
        if indecl:
            return "s. indecl."
        # ex.: "s. masc. 2ª (-i)" / "s. fem. 1ª (-ae)"
        g = {"m": "masc.", "f": "fem.", "n": "neut."}.get(gender, None)
        if decl_class and g:
            return f"s. {g} {decl_class}"
        if decl_class:
            return f"s. {decl_class}"
        if g:
            return f"s. {g}"
        return "s."

    if pos == "VERB":
        if verb_class:
            return verb_class
        return "v."

    if pos == "ADJ":
        if adj_class:
            return adj_class
        return "adj."

    base = {
        "ADV": "adv.",
        "PREP": "prep.",
        "CONJ": "conj.",
        "INTERJ": "interj.",
    }.get(pos, None)
    return base


# --------- DDL novo esquema ---------
DDL_CREATE = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS source (
  source_id   INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  edition     TEXT,
  license     TEXT,
  uri         TEXT
);

CREATE TABLE IF NOT EXISTS entry (
  entry_id      INTEGER PRIMARY KEY,
  id            TEXT UNIQUE,
  source_id     INTEGER DEFAULT 0 REFERENCES source(source_id),
  lemma         TEXT NOT NULL,
  lemma_sort    TEXT NOT NULL,
  homograph_no  INTEGER,
  pos           TEXT,
  gender        TEXT,
  indeclinable  INTEGER DEFAULT 0,
  decl_class    TEXT,
  verb_class    TEXT,
  adj_class     TEXT,
  morph_extra   TEXT NOT NULL DEFAULT '[]',
  morph_render  TEXT,
  definicao     TEXT NOT NULL,
  notas         TEXT,
  conf          TEXT,
  needs_review  INTEGER DEFAULT 0,
  redirect_only INTEGER DEFAULT 0,
  morph_out_of_vocab INTEGER DEFAULT 0,
  raw_json      TEXT NOT NULL,
  head_raw      TEXT,
  updated_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_entry_lemma_sort ON entry(lemma_sort);
CREATE INDEX IF NOT EXISTS idx_entry_pos ON entry(pos);

CREATE TABLE IF NOT EXISTS entry_form (
  form_id     INTEGER PRIMARY KEY,
  entry_id    INTEGER NOT NULL REFERENCES entry(entry_id) ON DELETE CASCADE,
  form        TEXT NOT NULL,
  form_norm   TEXT NOT NULL,
  kind        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_form_norm ON entry_form(form_norm);

CREATE TABLE IF NOT EXISTS sense (
  sense_id    INTEGER PRIMARY KEY,
  entry_id    INTEGER NOT NULL REFERENCES entry(entry_id) ON DELETE CASCADE,
  parent_id   INTEGER REFERENCES sense(sense_id) ON DELETE CASCADE,
  n_label     TEXT,
  level       INTEGER,
  gloss       TEXT,
  gloss_raw   TEXT
);
CREATE INDEX IF NOT EXISTS idx_sense_entry ON sense(entry_id);

CREATE VIRTUAL TABLE IF NOT EXISTS entry_fts USING fts5(
  lemma, forms, gloss, notes,
  content='',
  tokenize='unicode61 remove_diacritics 2'
);
CREATE TABLE IF NOT EXISTS entry_fts_data(id INTEGER PRIMARY KEY, block BLOB);
CREATE TABLE IF NOT EXISTS entry_fts_docsize(id INTEGER PRIMARY KEY, sz BLOB);
CREATE TABLE IF NOT EXISTS entry_fts_config(k PRIMARY KEY, v) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS entry_fts_idx(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID;

-- View de compatibilidade com o schema legado
DROP VIEW IF EXISTS entries;
CREATE VIEW entries AS
SELECT
  id,
  lemma                AS lema_canonico,
  morph_render         AS morfologia,
  definicao,
  notas,
  conf,
  needs_review,
  redirect_only,
  morph_out_of_vocab,
  raw_json
FROM entry;
"""

def create_schema(conn_out: sqlite3.Connection):
    conn_out.executescript(DDL_CREATE)
    conn_out.execute("INSERT OR IGNORE INTO source(source_id,name) VALUES(0,'retificado');")
    conn_out.commit()

# --------- Populate FTS ---------
def rebuild_fts(conn_out: sqlite3.Connection):
    conn_out.execute("DELETE FROM entry_fts;")
    # forms agregadas
    conn_out.executescript("""
    INSERT INTO entry_fts(rowid, lemma, forms, gloss, notes)
    SELECT e.entry_id,
           e.lemma,
           COALESCE((
               SELECT GROUP_CONCAT(f.form, ' | ')
               FROM entry_form f
               WHERE f.entry_id = e.entry_id
           ), e.lemma),
           e.definicao,
           e.notas
    FROM entry e;
    """)
    conn_out.commit()

# --------- Migração ---------
def migrate(in_db: str, out_db: str, dry_run_csv: Optional[str], apply_flag: bool):
    # abrir entrada
    cin = sqlite3.connect(in_db); cin.row_factory = sqlite3.Row
    # saída (novo arquivo)
    if apply_flag:
        if os.path.exists(out_db):
            os.remove(out_db)
        cout = sqlite3.connect(out_db); cout.row_factory = sqlite3.Row
        create_schema(cout)
    else:
        cout = None

    # ler legado
    rows = cin.execute("""
        SELECT id, lema_canonico, morfologia, definicao, notas, conf,
               needs_review, redirect_only, morph_out_of_vocab, raw_json
        FROM entries
    """).fetchall()

    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    preview = []
    inserted = 0

    if apply_flag:
        cout.create_function("UNACCENT_LOWER", 1, unaccent_lower, deterministic=True)

    for r in rows:
        rid = r["id"]
        lemma = r["lema_canonico"] or ""
        lemma_sort = norm_lemma(lemma) or ""
        definicao = r["definicao"] or ""
        notas = r["notas"]
        conf = r["conf"]
        needs_review = int(r["needs_review"] or 0)
        redirect_only = int(r["redirect_only"] or 0)
        morph_oov = int(r["morph_out_of_vocab"] or 0)
        raw_json = r["raw_json"] or ""

        pos, gender, decl_class, verb_class, adj_class, morph_extra, indecl, morph_render_old = parse_morfologia(r["morfologia"])
        # se morph_render antigo era vazio/inútil, renderizar mínimo
        if not morph_render_old or not morph_render_old.strip():
            morph_render = render_morph(pos, gender, decl_class, verb_class, adj_class, indecl)
        else:
            morph_render = morph_render_old

        preview.append({
            "id": rid,
            "lemma": lemma,
            "lemma_sort": lemma_sort,
            "pos": pos or "",
            "gender": gender or "",
            "indeclinable": indecl,
            "decl_class": decl_class or "",
            "verb_class": verb_class or "",
            "adj_class": adj_class or "",
            "morph_extra": morph_extra,
            "morph_render": morph_render or "",
            "needs_review": needs_review,
            "redirect_only": redirect_only,
            "morph_out_of_vocab": morph_oov,
        })

        if apply_flag:
            cur = cout.cursor()
            cur.execute("""
                INSERT INTO entry (
                  id, source_id, lemma, lemma_sort,
                  pos, gender, indeclinable, decl_class, verb_class, adj_class,
                  morph_extra, morph_render, definicao, notas, conf, needs_review,
                  redirect_only, morph_out_of_vocab, raw_json, head_raw, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                rid, 0, lemma, lemma_sort,
                pos, gender, indecl, decl_class, verb_class, adj_class,
                morph_extra, morph_render, definicao, notas, conf, needs_review,
                redirect_only, morph_oov, raw_json, None, now
            ))
            entry_id = cur.lastrowid

            # entry_form: pelo menos a forma ortográfica do lemma
            cur.execute("""
                INSERT INTO entry_form(entry_id, form, form_norm, kind)
                VALUES (?, ?, ?, 'orth')
            """, (entry_id, lemma, lemma_sort))
            inserted += 1

    # dry-run CSV
    if dry_run_csv:
        with open(dry_run_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(preview[0].keys()) if preview else
                               ["id","lemma","lemma_sort","pos","gender","indeclinable",
                                "decl_class","verb_class","adj_class","morph_extra",
                                "morph_render","needs_review","redirect_only","morph_out_of_vocab"])
            w.writeheader()
            w.writerows(preview)

    if apply_flag:
        cout.commit()
        rebuild_fts(cout)
        cout.close()

    cin.close()
    return {"rows": len(preview), "inserted": inserted}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_db", required=True, help="retificado.db legado (com tabela entries)")
    ap.add_argument("--out", dest="out_db", required=True, help="arquivo de saída (novo esquema)")
    ap.add_argument("--dry-run", dest="dry_run_csv", help="gera CSV com prévia, não escreve DB")
    ap.add_argument("--apply", action="store_true", help="cria/popula o novo DB")
    args = ap.parse_args()

    if not os.path.exists(args.in_db):
        print(f"ERRO: arquivo de entrada não existe: {args.in_db}", file=sys.stderr)
        sys.exit(2)

    if not args.apply and not args.dry_run_csv:
        print("Nada a fazer: use --dry-run preview.csv ou --apply", file=sys.stderr)
        sys.exit(2)

    res = migrate(args.in_db, args.out_db, args.dry_run_csv, args.apply)
    mode = "APPLY" if args.apply else "DRYRUN"
    print(f"[{mode}] rows={res['rows']} inserted={res['inserted']}")


if __name__ == "__main__":
    main()
