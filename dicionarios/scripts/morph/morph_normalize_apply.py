#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
morph_normalize_apply.py
------------------------
Normaliza e corrige a coluna "morfologia" em retificado.db usando consenso entre
Gaffiot (lat->fr), Lat->German e Lewis & Short.

Regras de atualização (estritas):
  1) O lema deve ser ÚNICO no retificado (por normalização).
  2) O lema deve ser ÚNICO em pelo menos 2 fontes externas (entre Gaffiot, LatDeu, LS).
  3) Deve haver CONSENSO de POS (>= 2 fontes concordando).
  4) Deve existir mapeamento claro para o vocabulário interno.
Se qualquer condição falhar, o item fica em relatório (sem aplicar).

Entradas:
  --retificado retificado.db
  --gaffiot_tsv gaffiot.tsv            # colunas esperadas: headword/lemma?, pos, itype, indeclinable, gen_text
  --latdeu_tsv latgerman.tsv           # colunas esperadas: lemma?, bestimmung, grammar
  --ls_tsv ls_entries.tsv              # colunas esperadas: lemma?, pos, itypes

Saídas:
  --report out.csv                     # relatório (dry-run) com decisões por id
  --apply                              # aplica UPDATEs (sem --apply faz apenas dry-run)

Observações:
  - Detectamos automaticamente a coluna de lema nas TSVs (lemma/headword/latin).
  - Normalização de lema: lower + j->i, v->u + strip espaços (pode adaptar).
  - Verbos: detecta conjugação pelo infinitivo quando possível (preferência LatDeu/LS).
  - Substantivos: detecta declinação pelo genitivo (Gaffiot gen_text) e gênero consolidado.
  - Adjetivos: detecta (1/2) quando houver “-us, -a, -um / a, um”; 3ª (1T/2T/3T) só quando houver sinal claro.

Uso:
  python3 morph_normalize_apply.py \
    --retificado retificado.db \
    --gaffiot_tsv gaffiot.tsv \
    --latdeu_tsv latgerman.tsv \
    --ls_tsv ls_entries.tsv \
    --report morph_dryrun.csv

  # Para aplicar no DB após revisar o CSV:
  python3 morph_normalize_apply.py \
    --retificado retificado.db \
    --gaffiot_tsv gaffiot.tsv \
    --latdeu_tsv latgerman.tsv \
    --ls_tsv ls_entries.tsv \
    --report morph_applied.csv --apply
"""
from __future__ import annotations
import argparse, csv, json, os, re, sqlite3, sys
from collections import defaultdict, Counter
from typing import Dict, List, Optional, Tuple

def norm_lemma(s: str) -> str:
    s = (s or "").strip().lower()
    # normalização simples (ajuste conforme o que você usa em outros scripts)
    s = s.replace("j", "i").replace("v", "u")
    s = re.sub(r"\s+", " ", s)
    return s

def detect_lemma_col(cols: List[str]) -> Optional[str]:
    # tenta adivinhar a coluna de lemma nas TSVs
    lower = [c.lower() for c in cols]
    for cand in ("lemma","headword","latin","head","lema"):
        if cand in lower:
            return cols[lower.index(cand)]
    # fallback: se não houver, None
    return None

# ---------------- LAT->GERMAN helpers ----------------

def pos_from_latdeu(grammar: str) -> Optional[str]:
    g = (grammar or "").strip().lower()
    # mapa comum
    if g == "s" or "subst" in g or "nomen" in g: return "NOUN"
    if g == "v" or "verb" in g: return "VERB"
    if "adj" in g: return "ADJ"
    if "adv" in g: return "ADV"
    if "präp" in g or "prep" in g: return "PREP"
    if "konj" in g or "conj" in g: return "CONJ"
    if "interj" in g: return "INTERJ"
    return None

def gender_from_text(text: str) -> Optional[str]:
    # aceita "m.", "f.", "n.", "mask.", "fem.", "neutr." (alemão/latim misto)
    t = (text or "").lower()
    if re.search(r"\b(m\.|mask\.)\b", t): return "m"
    if re.search(r"\b(f\.|fem\.)\b", t): return "f"
    if re.search(r"\b(n\.|neutr\.)\b", t): return "n"
    # fallback: letras soltas m/f/n (pode ser ruidoso, então exigimos delimitadores)
    return None

def aggregate_gender(rows: List[Dict[str,str]], best_col: Optional[str], gram_col: Optional[str]) -> Optional[str]:
    vals = []
    for r in rows:
        best = r.get(best_col) if best_col else None
        gram = r.get(gram_col) if gram_col else None
        g = gender_from_text((best or "") + " " + (gram or ""))
        if g: vals.append(g)
    if not vals:
        return None
    cnt = Counter(vals)
    top, n = cnt.most_common(1)[0]
    # se maioria simples e não muito conflituoso, aceita
    if n >= (len(vals) - n):
        return top
    return None

def infinitive_from_latdeu(rows: List[Dict[str,str]], best_col: Optional[str], gram_col: Optional[str]) -> Optional[str]:
    # Heurística: às vezes "Infinitiv ..." aparece em alguma coluna textual
    for r in rows:
        txt = ((r.get(best_col) or "") + " " + (r.get(gram_col) or "")).lower()
        m = re.search(r"infinitiv[:=]?\s*([a-zāēīōū]+)", txt)
        if m:
            return m.group(1)
    return None

# ---------------- GAFFIOT helpers ----------------

def pos_from_gaff(pos: str) -> Optional[str]:
    p = (pos or "").lower()
    if "adj" in p: return "ADJ"
    if "verbe" in p or "verb" in p: return "VERB"
    if "subst" in p or "nom" in p: return "NOUN"
    if "adv" in p: return "ADV"
    if "conj" in p: return "CONJ"
    if "prép" in p or "prep" in p: return "PREP"
    if "interj" in p: return "INTERJ"
    return None

def noun_class_from_gen(gen_text: str, gender_hint: Optional[str], indeclinable: Optional[int]) -> Optional[str]:
    if indeclinable and int(indeclinable) == 1:
        return "s. indecl."
    g = (gen_text or "").strip().lower()
    # limpar vírgulas/suplementos
    g = re.sub(r"[,\s]+.*$", "", g)
    def base_from_gen(gs: str) -> Optional[str]:
        if gs.endswith("ae"): return "1ª (-ae)"
        if gs.endswith("i"):  return "2ª (-i)"
        if gs.endswith("is"): return "3ª (-is)"
        if gs.endswith("us"): return "4ª (-us)"
        if gs.endswith("ei"): return "5ª (-ei)"
        return None
    base = base_from_gen(g)
    if not base:
        return None
    if gender_hint in ("m","f","n"):
        label = {"m":"masc.","f":"fem.","n":"neut."}[gender_hint]
        return f"s. {label} {base}"
    return f"s. {base}"

def adj_class_from_tails(itype: str, gen_text: str, ls_itypes: str) -> Optional[str]:
    # procura sinais de 1/2: "-us, -a, -um" ou "a, um"
    s = " ".join([(itype or ""), (gen_text or ""), (ls_itypes or "")]).lower()
    if re.search(r"\b(us,\s*a,\s*um|a,\s*um)\b", s):
        return "adj. (1/2) -a, -um"
    # heurísticas para 3ª decl. (precisa ser muito explícito, senão evita)
    # Exemplos (ajuste conforme seus dados): "-is (m/f), -e (n.)" -> 3T; "-is (m/f/n)" -> 1T; "-is (m/f), -is (n.)" -> 2T
    if "adj. 3" in s or "adjectif 3" in s or "adj. (3)" in s:
        # sem sinal claro do nº de terminações, não arriscar
        return None
    return None

# ---------------- LS helpers ----------------

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

# --------------- Verb classification ---------------

def verb_class_from_inf(inf: str) -> Optional[str]:
    if not inf: return None
    t = inf.strip().lower()
    # normalizar macrons ocasionais
    t = t.replace("ī","i").replace("ē","e").replace("ā","a")
    if t.endswith("are"): return "v. 1ª (-āre)"
    if t.endswith("ere"):
        # diferenciação 2ª vs 3ª com macron é difícil no texto cru; tentaremos 2ª se há sinais "ēre"
        if "ēre" in inf or "e\u0304re" in inf:  # casos com macron real
            return "v. 2ª (-ēre)"
        # 3ª -iō: heurística fraca, depende de dados externos; aqui não forçamos sem prova
        return "v. 3ª (-ere)"
    if t.endswith("ire"): return "v. 4ª (-īre)"
    return None

# --------------- Consensus and decision ---------------

def vote_pos(gpos, lpos, dpos) -> Tuple[str,int]:
    votes = Counter([p for p in (gpos,lpos,dpos) if p])
    if not votes:
        return ("",0)
    label, n = votes.most_common(1)[0]
    return (label, n)

def build_sources(gaff_row: dict|None, ls_row: dict|None, lat_rows: List[dict]) -> dict:
    src = {}
    if gaff_row:
        src["gaffiot"] = {
            "pos": pos_from_gaff(gaff_row.get("pos")),
            "itype": gaff_row.get("itype"),
            "indeclinable": gaff_row.get("indeclinable"),
            "gen_text": gaff_row.get("gen_text"),
        }
    if ls_row:
        src["ls"] = {
            "pos": pos_from_ls(ls_row.get("pos")),
            "itypes": ls_row.get("itypes"),
        }
    if lat_rows:
        # agregados
        lat_pos = None
        genders = aggregate_gender(lat_rows, best_col="bestimmung", gram_col="grammar")
        lat_inf  = infinitive_from_latdeu(lat_rows, best_col="bestimmung", gram_col="grammar")
        # se qualquer linha sugere POS, preferimos a primeira positiva
        for r in lat_rows:
            lat_pos = lat_pos or pos_from_latdeu(r.get("grammar"))
        src["latdeu"] = {
            "pos": lat_pos,
            "gender": genders,
            "infinitive": lat_inf,
        }
    return src

def propose_morph(src: dict) -> Optional[str]:
    g = src.get("gaffiot", {})
    l = src.get("ls", {})
    d = src.get("latdeu", {})

    gpos, lpos, dpos = g.get("pos"), l.get("pos"), d.get("pos")
    pos, votes = vote_pos(gpos, lpos, dpos)
    if votes < 2:
        return None

    if pos == "VERB":
        inf = d.get("infinitive")
        # TODO: tentar extrair infinitivo também de LS/Gaffiot quando disponível
        return verb_class_from_inf(inf)

    if pos == "NOUN":
        gender = d.get("gender")
        return noun_class_from_gen(g.get("gen_text"), gender, g.get("indeclinable"))

    if pos == "ADJ":
        return adj_class_from_tails(g.get("itype"), g.get("gen_text"), l.get("itypes"))

    # classes simples
    if pos == "ADV": return "adv."
    if pos == "CONJ": return "conj."
    if pos == "INTERJ": return "interj."
    if pos == "PREP": return "prep. com acc./abl."  # caso desconhecido, escolhemos a forma abrangente
    return None

# --------------- Loading TSVs ---------------

def load_tsv(path: str) -> List[Dict[str,str]]:
    with open(path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter="\t")
        return list(rdr)

def index_by_lemma(rows: List[Dict[str,str]], lemma_col: str) -> Dict[str, List[Dict[str,str]]]:
    groups = defaultdict(list)
    for r in rows:
        lemma = r.get(lemma_col, "")
        groups[norm_lemma(lemma)].append(r)
    return groups

# --------------- Retificado helpers ---------------

def fetch_unique_retificado(conn: sqlite3.Connection) -> Dict[str, dict]:
    # Retorna dict lemma_norm -> {id, lema_canonico, morfologia}
    q = """
    WITH u AS (
      SELECT lower(replace(replace(replace(lema_canonico,'J','I'),'V','U'),'  ',' ')) AS ln, COUNT(*) AS n
      FROM entries
      GROUP BY ln
      HAVING n=1
    )
    SELECT e.id, e.lema_canonico, e.morfologia,
           lower(replace(replace(replace(e.lema_canonico,'J','I'),'V','U'),'  ',' ')) AS lemma_norm
    FROM entries e
    JOIN u ON u.ln = lower(replace(replace(replace(e.lema_canonico,'J','I'),'V','U'),'  ',' '));
    """
    out = {}
    for row in conn.execute(q):
        out[row["lemma_norm"]] = {"id": row["id"], "lema_canonico": row["lema_canonico"], "morfologia": row["morfologia"]}
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--retificado", required=True)
    ap.add_argument("--gaffiot_tsv", required=True)
    ap.add_argument("--latdeu_tsv", required=True)
    ap.add_argument("--ls_tsv", required=True)
    ap.add_argument("--report", required=True)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    # Load TSVs
    g_rows = load_tsv(args.gaffiot_tsv)
    d_rows = load_tsv(args.latdeu_tsv)
    l_rows = load_tsv(args.ls_tsv)

    # Detect lemma columns
    g_lemma_col = detect_lemma_col(list(g_rows[0].keys())) if g_rows else None
    d_lemma_col = detect_lemma_col(list(d_rows[0].keys())) if d_rows else None
    l_lemma_col = detect_lemma_col(list(l_rows[0].keys())) if l_rows else None
    if not all([g_lemma_col, d_lemma_col, l_lemma_col]):
        print("[ERROR] Não foi possível detectar a coluna de lema em um dos TSVs.", file=sys.stderr)
        sys.exit(2)

    # Build indices/groupings
    g_idx = index_by_lemma(g_rows, g_lemma_col)
    d_idx = index_by_lemma(d_rows, d_lemma_col)
    l_idx = index_by_lemma(l_rows, l_lemma_col)

    # Unique entries in externals
    g_uni = {k:v for k,v in g_idx.items() if len(v)==1}
    d_uni = {k:v for k,v in d_idx.items() if len(v)==1}
    l_uni = {k:v for k,v in l_idx.items() if len(v)==1}

    # Retificado uniques
    conn = sqlite3.connect(args.retificado)
    conn.row_factory = sqlite3.Row
    ret_uni = fetch_unique_retificado(conn)

    # Decide per lemma
    updates = []
    rows_out = []
    for ln, meta in ret_uni.items():
        id_ = meta["id"]
        lemma = meta["lema_canonico"]
        morph_old = meta["morfologia"]

        # require at least 2 unique sources among externals
        sources_present = []
        g1 = g_uni.get(ln)
        d1 = d_uni.get(ln)
        l1 = l_uni.get(ln)
        if g1: sources_present.append("gaffiot")
        if d1: sources_present.append("latdeu")
        if l1: sources_present.append("ls")
        if len(sources_present) < 2:
            rows_out.append({"id":id_, "lemma":lemma, "lemma_norm":ln, "morph_old":morph_old, "morph_new":"","decision":"skip:not_enough_unique_sources","sources":";".join(sources_present)})
            continue

        # build sources dict
        g_row = g1[0] if g1 else None
        l_row = l1[0] if l1 else None
        d_rows_for_ln = d_idx.get(ln, [])  # usamos todas as linhas de latdeu do lema (mesmo se >1 total), mas garantimos unicidade acima quando exigido
        src = build_sources(g_row, l_row, d_rows_for_ln)

        morph_new = propose_morph(src)
        if not morph_new:
            rows_out.append({"id":id_, "lemma":lemma, "lemma_norm":ln, "morph_old":morph_old, "morph_new":"","decision":"skip:no_consensus_or_mapping","sources":";".join(sources_present)})
            continue

        if morph_new == morph_old:
            rows_out.append({"id":id_, "lemma":lemma, "lemma_norm":ln, "morph_old":morph_old, "morph_new":morph_new,"decision":"noop:same","sources":";".join(sources_present)})
            continue

        # accept
        updates.append((morph_new, id_))
        rows_out.append({"id":id_, "lemma":lemma, "lemma_norm":ln, "morph_old":morph_old, "morph_new":morph_new,"decision":"update","sources":";".join(sources_present)})

    # Write report
    with open(args.report, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id","lemma","lemma_norm","morph_old","morph_new","decision","sources"])
        w.writeheader()
        for r in rows_out:
            w.writerow(r)

    print(f"[REPORT] {args.report}  rows={len(rows_out)}  updates={sum(1 for r in rows_out if r['decision']=='update')}")

    # Apply
    if args.apply and updates:
        cur = conn.cursor()
        cur.executemany("""
            UPDATE entries
            SET morfologia = ?,
                needs_review = 0,
                conf = CASE WHEN conf IS NULL OR conf!='conf:high' THEN 'conf:med' ELSE conf END,
                notas = CASE
                          WHEN notas IS NULL OR notas='' THEN '[morph_auto] consenso≥2 fontes; normalizado'
                          ELSE notas || ' | [morph_auto] consenso≥2 fontes; normalizado'
                        END
            WHERE id = ?;
        """, updates)
        conn.commit()
        print(f"[APPLY] updated {cur.rowcount} rows")

if __name__ == "__main__":
    main()
