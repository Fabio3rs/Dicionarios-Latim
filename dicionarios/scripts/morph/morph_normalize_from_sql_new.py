#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
morph_normalize_from_sql_new.py
-------------------------------
Suporta **apenas** os formatos novos de banco:
- retificado.db (tabela `entries`)
- gaffiot.db (tabela `entry` com colunas lemma_sort, pos, itype, gen_text, indeclinable)
- ls_dict.db  (tabela `entry` com colunas lemma_sort, pos, itype, itype_json, gen_text, indeclinable)
- token_latim_german.sqlite (VOC/FORM) com VOC.grammar, VOC.latin, FORM.bestimmung (e vok_id)

Pipeline:
1) Normaliza lema (j→i, v→u, sem acento) e coleta **apenas lemas únicos** no retificado.
2) Busca o mesmo lema nas três fontes externas (apenas quando únicos por fonte).
3) Consolida POS por votação (consenso ≥2 entre Gaffiot, LS e Lat→Deu).
4) Deriva morfologia segundo o vocabulário alvo do projeto:
   - Verbos: pela detecção de infinitivo em Lat→Deu (VOC.latin/FORM.bestimmung).
   - Substantivos: pela terminação do gen. sg. (Gaffiot.gen_text; fallback de VOC.latin) + gênero (FORM.bestimmung/VOC.grammar/VOC.latin).
   - Adjetivos: (1/2) se detectado “a, um” em caudas/itype/itype_json/latin; caso contrário tenta padrão.
   - Demais classes: adv./conj./interj./prep.
5) CSV de auditoria (completo) e, se --apply, UPDATE em retificado com notas/flags leves.

Uso (dry-run):
  python3 morph_normalize_from_sql_new.py \
    --retificado retificado.db \
    --gaffiot_db gaffiot.db \
    --latdeu_db token_latim_german.sqlite \
    --ls_db ls_dict.db \
    --report morph_sql_full.csv

Aplicando:
  python3 morph_normalize_from_sql_new.py \
    --retificado retificado.db \
    --gaffiot_db gaffiot.db \
    --latdeu_db token_latim_german.sqlite \
    --ls_db ls_dict.db \
    --report morph_sql_applied.csv --apply
"""
from __future__ import annotations
import argparse, csv, re, sqlite3
from collections import defaultdict, Counter
from typing import Dict, List, Optional, Tuple
import unicodedata
import json

# -------------------------- utilidades ---------------------------
def unaccent_lower(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.casefold()

def norm_lemma_py(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("J","I").replace("j","i").replace("V","U").replace("v","u")
    s = re.sub(r"\s+", " ", s)
    return unaccent_lower(s) or ""

# -------------------------- POS mappers --------------------------
def pos_from_gaff(pos: str) -> Optional[str]:
    p = (pos or "").lower()
    if "adj" in p: return "ADJ"
    if "verb" in p or "verbe" in p: return "VERB"
    if "subst" in p or "nom" in p or "noun" in p: return "NOUN"
    if "adv" in p: return "ADV"
    if "prep" in p or "prép" in p: return "PREP"
    if "conj" in p: return "CONJ"
    if "interj" in p: return "INTERJ"
    return None

def pos_from_ls(pos: str) -> Optional[str]:
    p = (pos or "").strip().lower()
    if "verb" in p: return "VERB"
    if "noun" in p or "subst" in p: return "NOUN"
    if "adj" in p: return "ADJ"
    if "adv" in p: return "ADV"
    if "prep" in p: return "PREP"
    if "conj" in p: return "CONJ"
    if "interj" in p: return "INTERJ"
    return None

def pos_from_latdeu(grammar: str) -> Optional[str]:
    g = (grammar or "").strip().lower()
    if g == "s" or "subst" in g or "nomen" in g: return "NOUN"
    if g == "v" or "verb" in g: return "VERB"
    if "adj" in g: return "ADJ"
    if "adv" in g: return "ADV"
    if "präp" in g or "prep" in g: return "PREP"
    if "konj" in g or "conj" in g: return "CONJ"
    if "interj" in g: return "INTERJ"
    return None

# ---------------------- pistas do Lat->Deu -----------------------
GEN_PAT = re.compile(r"\\b(m\\.|mask\\.|männ\\.)\\b", re.I)
FEM_PAT = re.compile(r"\\b(f\\.|fem\\.|weib\\.)\\b", re.I)
NEU_PAT = re.compile(r"\\b(n\\.|neutr\\.|sächl\\.)\\b", re.I)

def gender_from_text(text: str) -> Optional[str]:
    t = (text or "")
    if GEN_PAT.search(t): return "m"
    if FEM_PAT.search(t): return "f"
    if NEU_PAT.search(t): return "n"
    return None

def aggregate_gender(rows: List[dict], best_col: str, gram_col: str, latin_col: str) -> Optional[str]:
    vals = []
    for r in rows:
        big = " ".join([str(r.get(best_col) or ""), str(r.get(gram_col) or ""), str(r.get(latin_col) or "")])
        g = gender_from_text(big)
        if g: vals.append(g)
    if not vals: return None
    cnt = Counter(vals)
    top, n = cnt.most_common(1)[0]
    if n >= (len(vals) - n):
        return top
    return None

INF_PAT = re.compile(r"\\b(infinitiv|inf\\.)[:=]?\\s*([a-zāēīōū]+)", re.I)
ADJ12_PAT = re.compile(r"\\b([-,\\s])a[,\\s]+um\\b", re.I)

def infinitive_from_text(rows: List[dict], text_cols: List[str]) -> Optional[str]:
    for r in rows:
        big = " ".join([str(r.get(c) or "") for c in text_cols])
        m = INF_PAT.search(big.lower())
        if m: return m.group(2)
    return None

def detect_adj12_from_text(rows: List[dict], text_cols: List[str]) -> bool:
    for r in rows:
        big = " ".join([str(r.get(c) or "") for c in text_cols])
        if ADJ12_PAT.search(big):
            return True
    return False

def parse_lat_latin(latin: str) -> dict:
    """Extrai pequenas pistas de VOC.latin tipo 'frugiparus a um' ou 'aemulare, ...'"""
    s = (latin or "")
    out = {"pos_hint": None, "gen_sg": None, "gender": None, "inf": None, "adj12": False}
    if ADJ12_PAT.search(s): out["adj12"] = True
    # infinitivo heurístico: token com -are/-ere/-ire
    m = re.search(r"\\b([a-zāēīōū]+(?:āre|ēre|ere|īre))\\b", s.lower())
    if m: out["inf"] = m.group(1)
    # genitivo singular simples após vírgula: '-ae', '-i', '-is', '-us', '-ei'
    m2 = re.search(r"[,;]\\s*([a-z]+)$", unaccent_lower(s) or "")
    if m2: out["gen_sg"] = m2.group(1)
    # gênero explícito no final 'm, f, n' etc.
    g = gender_from_text(s)
    if g: out["gender"] = g
    # pos hint
    if " adv" in s.lower() or s.strip().endswith("(Adv.)"):
        out["pos_hint"] = "ADV"
    return out

# ---------------------- classes alvo do projeto ------------------
def noun_class_from_gen(gen_text: Optional[str], gender_hint: Optional[str], indeclinable: Optional[int]) -> Optional[str]:
    if indeclinable and int(indeclinable) == 1:
        return "s. indecl."
    g = (gen_text or "").strip().lower()
    g = re.sub(r"[\\s,;].*$", "", g)  # pega só o sufixo genitivo
    def base_from_gen(gs: str) -> Optional[str]:
        if gs.endswith("ae"): return "1ª (-ae)"
        if gs.endswith("i"):  return "2ª (-i)"
        if gs.endswith("is"): return "3ª (-is)"
        if gs.endswith("us"): return "4ª (-us)"
        if gs.endswith("ei"): return "5ª (-ei)"
        return None
    base = base_from_gen(g)
    if not base: return None
    if gender_hint in ("m","f","n"):
        label = {"m":"masc.","f":"fem.","n":"neut."}[gender_hint]
        return f"s. {label} {base}"
    return f"s. {base}"

def verb_class_from_inf(inf: Optional[str]) -> Optional[str]:
    if not inf: return None
    t = (inf or "").strip().lower()
    t = t.replace("ī","i").replace("ē","e").replace("ā","a")
    if t.endswith("are"): return "v. 1ª (-āre)"
    if t.endswith("ire"): return "v. 4ª (-īre)"
    if t.endswith("ere"):
        # 2ª vs 3ª: sem macron confiável, assumimos 3ª (conservador)
        return "v. 3ª (-ere)"
    return None

def adj_class_from_signals(itype: Optional[str], gen_text: Optional[str], itype_json: Optional[str], latin_sniff: bool) -> Optional[str]:
    s = " ".join([str(itype or ""), str(gen_text or ""), str(itype_json or "")]).lower()
    if ADJ12_PAT.search(s) or latin_sniff:
        return "adj. (1/2) -a, -um"
    return None

def other_class(pos: str) -> Optional[str]:
    return {
        "ADV":"adv.", "CONJ":"conj.", "INTERJ":"interj.",
        "PREP":"prep. com acc./abl."
    }.get(pos)

def vote_pos(gpos, lpos, dpos) -> Tuple[str,int]:
    votes = Counter([p for p in (gpos,lpos,dpos) if p])
    if not votes: return ("",0)
    label, n = votes.most_common(1)[0]
    return (label, n)

# --------------------- loaders (formatos novos) ------------------
def fetch_retificado_unique(conn: sqlite3.Connection) -> Dict[str, dict]:
    out = {}
    q = """
    WITH t AS (
      SELECT UNACCENT_LOWER(REPLACE(REPLACE(REPLACE(lema_canonico,'J','I'),'V','U'),'  ',' ')) AS ln,
             COUNT(*) AS n
      FROM entries GROUP BY ln
    )
    SELECT e.id, e.lema_canonico, e.morfologia,
           UNACCENT_LOWER(REPLACE(REPLACE(REPLACE(e.lema_canonico,'J','I'),'V','U'),'  ',' ')) AS lemma_norm
    FROM entries e
    JOIN t ON t.ln = UNACCENT_LOWER(REPLACE(REPLACE(REPLACE(e.lema_canonico,'J','I'),'V','U'),'  ',' '))
    WHERE t.n=1;
    """
    for row in conn.execute(q):
        out[row["lemma_norm"]] = {"id": row["id"], "lema_canonico": row["lema_canonico"], "morfologia": row["morfologia"]}
    return out

def fetch_gaffiot_unique(conn: sqlite3.Connection) -> Dict[str, dict]:
    q = """
    WITH g AS (
      SELECT
        UNACCENT_LOWER(lemma_sort) AS ln,
        pos, itype, gen_text, indeclinable
      FROM entry
    ),
    u AS (SELECT ln, COUNT(*) AS n FROM g GROUP BY ln HAVING n=1)
    SELECT g.* FROM g JOIN u USING(ln);
    """
    return {r["ln"]: dict(r) for r in conn.execute(q)}

def fetch_ls_unique(conn: sqlite3.Connection) -> Dict[str, dict]:
    # Novo LS muito parecido com Gaffiot
    q = """
    WITH l AS (
      SELECT
        UNACCENT_LOWER(lemma_sort) AS ln,
        pos, itype, itype_json, gen_text, indeclinable
      FROM entry
    ),
    u AS (SELECT ln, COUNT(*) AS n FROM l GROUP BY ln HAVING n=1)
    SELECT l.* FROM l JOIN u USING(ln);
    """
    return {r["ln"]: dict(r) for r in conn.execute(q)}

def fetch_latdeu_grouped(conn: sqlite3.Connection) -> Dict[str, List[dict]]:
    # Junta todas as linhas por paradigma (vok_id) e por lemma_norm (preferindo VOC.form_norm; fallback VOC.lemma)
    q = """
    SELECT
      UNACCENT_LOWER(COALESCE(FORM.form_norm, FORM.form)) AS ln,
      VOC.vok_id AS vok,
      VOC.grammar AS grammar,
      VOC.latin   AS latin,
      FORM.bestimmung AS bestimmung
    FROM VOC
    LEFT JOIN FORM USING(vok_id);
    """
    groups = defaultdict(list)
    for r in conn.execute(q):
        groups[r["ln"]].append(dict(r))
    return groups

def is_latdeu_unique(groups: Dict[str, List[dict]], ln: str) -> bool:
    rows = groups.get(ln, [])
    if not rows: return False
    vok_ids = {r.get("vok") for r in rows if r.get("vok") is not None}
    return len(vok_ids) == 1

# ----------------------------- main ------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--retificado", required=True)
    ap.add_argument("--gaffiot_db", required=True)
    ap.add_argument("--latdeu_db", required=True)
    ap.add_argument("--ls_db", required=True)
    ap.add_argument("--report", required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    # abre conexões
    ret = sqlite3.connect(args.retificado); ret.row_factory = sqlite3.Row
    gdb = sqlite3.connect(args.gaffiot_db); gdb.row_factory = sqlite3.Row
    lsd = sqlite3.connect(args.ls_db);      lsd.row_factory = sqlite3.Row
    ldd = sqlite3.connect(args.latdeu_db);  ldd.row_factory = sqlite3.Row

    # registrar função de normalização no SQLite
    for c in (ret, gdb, lsd, ldd):
        c.create_function("UNACCENT_LOWER", 1, unaccent_lower, deterministic=True)

    # coleções
    ret_uni = fetch_retificado_unique(ret)
    g_uni   = fetch_gaffiot_unique(gdb)
    l_uni   = fetch_ls_unique(lsd)
    d_grp   = fetch_latdeu_grouped(ldd)

    # loop e decisões
    updates = []
    rows_out = []
    for ln, meta in ret_uni.items():
        rid   = meta["id"]
        lemma = meta["lema_canonico"]
        morph_old = meta["morfologia"]

        # fontes presentes e únicas
        sources = []
        g_row = g_uni.get(ln)
        if g_row: sources.append("gaffiot")
        l_row = l_uni.get(ln)
        if l_row: sources.append("ls")
        d_unique = is_latdeu_unique(d_grp, ln)
        d_rows = d_grp.get(ln, []) if d_unique else []
        if d_unique: sources.append("latdeu")

        if len(sources) < 2:
            rows_out.append({
                "id": rid, "lemma": lemma, "lemma_norm": ln,
                "morph_old": morph_old, "morph_new": "",
                "decision": "skip:not_enough_unique_sources",
                "sources": ";".join(sources),
                "g_pos": g_row["pos"] if g_row else "",
                "l_pos": l_row["pos"] if l_row else "",
                "d_grammar": "|".join({str(r.get("grammar") or "") for r in d_rows}) if d_rows else "",
                "d_bestimmung": "|".join({str(r.get("bestimmung") or "") for r in d_rows}) if d_rows else "",
                "d_latin": "|".join({str(r.get("latin") or "") for r in d_rows}) if d_rows else "",
                "g_gen_text": g_row["gen_text"] if g_row else "",
                "l_gen_text": l_row["gen_text"] if l_row else "",
                "g_itype": g_row["itype"] if g_row else "",
                "l_itype": l_row["itype"] if l_row else "",
                "l_itype_json": l_row["itype_json"] if l_row else ""
            })
            continue

        # POS hints
        gpos = pos_from_gaff(g_row["pos"]) if g_row else None
        lpos = pos_from_ls(l_row["pos"]) if l_row else None
        dpos = None
        for r in d_rows:
            dpos = dpos or pos_from_latdeu(r.get("grammar"))

        # sinais auxiliares Lat->Deu
        lat_gender = aggregate_gender(d_rows, "bestimmung", "grammar", "latin") if d_rows else None
        lat_inf = infinitive_from_text(d_rows, ["bestimmung", "grammar", "latin"]) if d_rows else None
        lat_adj12 = detect_adj12_from_text(d_rows, ["latin"]) if d_rows else False

        # também faremos sniff de VOC.latin linha a linha
        pos_hint_lat = None
        gen_sg_lat = None
        gender_lat = None
        inf_lat = None
        adj12_lat = False
        for r in d_rows:
            info = parse_lat_latin(r.get("latin") or "")
            pos_hint_lat = pos_hint_lat or info["pos_hint"]
            gen_sg_lat   = gen_sg_lat   or info["gen_sg"]
            gender_lat   = gender_lat   or info["gender"]
            inf_lat      = inf_lat      or info["inf"]
            adj12_lat    = adj12_lat or info["adj12"]
        if pos_hint_lat and not dpos:
            dpos = pos_hint_lat

        # votação POS
        pos_label, votes = vote_pos(gpos, lpos, dpos)
        if votes < 2:
            rows_out.append({
                "id": rid, "lemma": lemma, "lemma_norm": ln,
                "morph_old": morph_old, "morph_new": "",
                "decision": "skip:no_pos_consensus",
                "sources": ";".join(sources),
                "g_pos": g_row["pos"] if g_row else "",
                "l_pos": l_row["pos"] if l_row else "",
                "d_grammar": "|".join({str(r.get("grammar") or "") for r in d_rows}) if d_rows else "",
                "d_bestimmung": "|".join({str(r.get("bestimmung") or "") for r in d_rows}) if d_rows else "",
                "d_latin": "|".join({str(r.get("latin") or "") for r in d_rows}) if d_rows else "",
                "g_gen_text": g_row["gen_text"] if g_row else "",
                "l_gen_text": l_row["gen_text"] if l_row else "",
                "g_itype": g_row["itype"] if g_row else "",
                "l_itype": l_row["itype"] if l_row else "",
                "l_itype_json": l_row["itype_json"] if l_row else ""
            })
            continue

        # decisão de morfologia
        morph_new = None
        if pos_label == "VERB":
            candidate_inf = inf_lat or lat_inf
            morph_new = verb_class_from_inf(candidate_inf)

        elif pos_label == "NOUN":
            gender_final = lat_gender or gender_lat
            gen_text_gaf = g_row["gen_text"] if g_row else None
            if not gen_text_gaf and gen_sg_lat:
                gen_text_gaf = "-" + gen_sg_lat
            indecl = (g_row["indeclinable"] if g_row else None) or (l_row["indeclinable"] if l_row else None)
            morph_new = noun_class_from_gen(gen_text_gaf, gender_final, indecl)

        elif pos_label == "ADJ":
            morph_new = adj_class_from_signals(
                g_row["itype"] if g_row else None,
                g_row["gen_text"] if g_row else None,
                l_row["itype_json"] if l_row else None,
                latin_sniff = (lat_adj12 or adj12_lat),
            )

        else:
            morph_new = other_class(pos_label)

        decision = ""
        if not morph_new:
            decision = "skip:no_mapping"
        elif morph_new == morph_old:
            decision = "noop:same"
        else:
            decision = "update"
            updates.append((morph_new, rid))

        rows_out.append({
            "id": rid, "lemma": lemma, "lemma_norm": ln,
            "morph_old": morph_old, "morph_new": morph_new or "",
            "decision": decision, "sources": ";".join(sources),
            # sinais brutos p/ auditoria
            "g_pos": g_row["pos"] if g_row else "",
            "l_pos": l_row["pos"] if l_row else "",
            "d_grammar": "|".join({str(r.get("grammar") or "") for r in d_rows}) if d_rows else "",
            "d_bestimmung": "|".join({str(r.get("bestimmung") or "") for r in d_rows}) if d_rows else "",
            "d_latin": "|".join({str(r.get("latin") or "") for r in d_rows}) if d_rows else "",
            "g_gen_text": g_row["gen_text"] if g_row else "",
            "l_gen_text": l_row["gen_text"] if l_row else "",
            "g_itype": g_row["itype"] if g_row else "",
            "l_itype": l_row["itype"] if l_row else "",
            "l_itype_json": l_row["itype_json"] if l_row else ""
        })

    # relatório completo
    fieldnames = [
        "id","lemma","lemma_norm","morph_old","morph_new","decision","sources",
        "g_pos","l_pos","d_grammar","d_bestimmung","d_latin",
        "g_gen_text","l_gen_text","g_itype","l_itype","l_itype_json"
    ]
    with open(args.report, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_out)
    print(f"[REPORT] {args.report} rows={len(rows_out)} updates={sum(1 for r in rows_out if r['decision']=='update')}")

    # aplicação
    if args.apply and updates:
        cur = ret.cursor()
        cur.executemany(
            """
            UPDATE entries
               SET morfologia = ?,
                   needs_review = 0,
                   conf = CASE WHEN conf IS NULL OR conf!='conf:high' THEN 'conf:med' ELSE conf END,
                   notas = CASE
                             WHEN notas IS NULL OR notas='' THEN '[morph_auto] consenso≥2 fontes; normalizado'
                             ELSE notas || ' | [morph_auto] consenso≥2 fontes; normalizado'
                           END
             WHERE id = ?;
            """,
            updates
        )
        ret.commit()
        print(f"[APPLY] updated {cur.rowcount} rows")

if __name__ == "__main__":
    main()
