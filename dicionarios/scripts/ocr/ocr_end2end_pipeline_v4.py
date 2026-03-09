#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ocr_end2end_pipeline_v2.py — pipeline com FAISS novo, LS em formato genérico (ao estilo Gaffiot) e retificado_v2.

- Seleciona suspeitos (auto) ou lê TSV.
- Sugere correções via:
    * variantes OCR/prefixos + match em LS (autodetect)
    * FAISS por definição/contexto (opcional) com reranking por similaridade de forma
    * Apoio morfológico/forma via token_latim_german.sqlite (Lat→Deu)
    * Apoio/fallback por Gaffiot (autodetect)

- Aplica com política conservadora e 4 "gates":
    1) Janela alfabética
    2) Guarda de afixo
    3) Piso de similaridade de caracteres
    4) Compatibilidade morfológica

Saídas:
- --dry-run (padrão): JSON por linha com diagnóstico.
- --apply: atualiza retificado_v2.db (cria backup em ./backups/).

Req:
- Python 3.11+
- pacotes: faiss-cpu (se usar --faiss-index), requests (Ollama)
"""

import argparse
import json
import os
import re
import shutil
import sqlite3
import time
import unicodedata
from typing import List, Dict, Tuple, Iterable, Optional

# =========================
# Normalização
# =========================

REMOVE_CHARS = r"[\^\·\.\(\)\[\]\{\}°˘̆̄¯´`ʹ,'’\"“”/\\;:!?*]"

CONFUSABLE_MAP = {
    "\u00e6": "ae",  # æ
    "\u0153": "oe",  # œ
    "\u0233": "y",  # ȳ
    "\u1e8f": "y",  # ẏ
    "\u1ef3": "y",  # ỳ
    "\u00fd": "y",  # ý
    "\u00ff": "y",  # ÿ
    "\u0177": "y",  # ŷ
    "\u1e99": "y",  # ẙ
    "\u0443": "y",  # у (cirílico)
    "\u03c5": "y",  # υ (grego)
    "\u03a5": "y",  # Υ
    "\u02bc": "'",  # ʼ
    "\u2019": "'",  # ’
    "\u2018": "'",  # ‘
    "\u2010": "-",  # ‐
    "\u2011": "-",  # non-breaking hyphen
    "\u2013": "-",  # –
    "\u2014": "-",  # —
    "\u00ad": "",  # soft hyphen
    "\u017f": "s",  # ſ
    "\u0131": "i",  # ı
    "\u0130": "i",  # İ
}
TRANSLATE_CONFUSIONS = str.maketrans(CONFUSABLE_MAP)


def strip_accents(s: str) -> str:
    result = "".join(
        c
        for c in unicodedata.normalize("NFD", s or "")
        if unicodedata.category(c) != "Mn"
    )
    result = result.translate(TRANSLATE_CONFUSIONS)
    return result


def norm_latin(s: str) -> str:
    x = strip_accents(s or "").casefold()
    x = re.sub(r"[^\w\s\-]", "", x, flags=re.UNICODE)
    x = re.sub(r"\s+", " ", x).strip()
    return x


def remove_symbols(s: Optional[str]) -> Optional[str]:
    if s is None:
        return s
    return norm_latin(re.sub(REMOVE_CHARS, "", s))


# æ -> a
def norm_latin_fusions(s: str) -> str:
    # s = norm_latin(s)
    s = s.replace("æ", "ae").replace("œ", "oe").strip()
    s = s.replace("Æ", "AE").replace("Œ", "OE").strip()
    return s


def unaccent_lower(s):
    if s is None:
        return None
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.translate(TRANSLATE_CONFUSIONS)
    return norm_latin_fusions(s.casefold())


def ratio(a: str, b: str) -> float:
    # similaridade por bigramas
    a = norm_latin(a or "")
    b = norm_latin(b or "")
    if not a and not b:
        return 1.0
    A = set(a[i : i + 2] for i in range(max(1, len(a) - 1)))
    B = set(b[i : i + 2] for i in range(max(1, len(b) - 1)))
    if not A and not B:
        return 1.0
    inter = len(A & B)
    uni = len(A | B) or 1
    return inter / uni


def edit_distance(a: str, b: str) -> int:
    a = a or ""
    b = b or ""
    la, lb = len(a), len(b)
    dp = list(range(lb + 1))
    for i in range(1, la + 1):
        prev = dp[0]
        dp[0] = i
        ca = a[i - 1]
        for j in range(1, lb + 1):
            temp = dp[j]
            cost = 0 if ca == b[j - 1] else 1
            dp[j] = min(dp[j] + 1, dp[j - 1] + 1, prev + cost)
            prev = temp
    return dp[lb]


# =========================
# Variantes OCR
# =========================

PREFIXES = [
    "ab",
    "ad",
    "con",
    "de",
    "dis",
    "ex",
    "in",
    "inter",
    "ob",
    "per",
    "post",
    "prae",
    "pro",
    "re",
    "sub",
    "super",
    "trans",
]

OCR_PAIRS = [
    ("o", "u"),
    ("u", "o"),
    ("rn", "m"),
    ("m", "rn"),
    ("cl", "d"),
    ("d", "cl"),
    ("æ", "ae"),
    ("œ", "oe"),
    ("j", "i"),
    ("v", "u"),
    ("ſ", "s"),
]


def gen_prefix_hyphen_variants(lemma: str) -> Iterable[str]:
    y_norm = norm_latin(lemma)
    yield lemma
    for p in PREFIXES:
        if y_norm.startswith(p) and len(y_norm) > len(p):
            yield lemma[: len(p)] + "-" + lemma[len(p) :]
            if len(lemma) > len(p) + 1 and lemma[len(p)] == "-":
                yield lemma.replace("-", "", 1)


def apply_ocr_swaps_once(s: str, a: str, b: str) -> List[str]:
    return [s.replace(a, b)] if a in s else []


def gen_ocr_variants(lemma: str, limit: int = 100) -> List[str]:
    cand = {lemma}
    for a, b in OCR_PAIRS:
        for base in list(cand):
            cand.update(apply_ocr_swaps_once(base, a, b))
    combo_added = set()
    base_list = list(cand)
    for i, (a1, b1) in enumerate(OCR_PAIRS):
        for a2, b2 in OCR_PAIRS[i + 1 :]:
            for s in base_list:
                t = s.replace(a1, b1)
                u = t.replace(a2, b2)
                if u != s and u not in cand:
                    combo_added.add(u)
                    if len(cand) + len(combo_added) >= limit:
                        break
            if len(cand) + len(combo_added) >= limit:
                break
        if len(cand) + len(combo_added) >= limit:
            break
    cand.update(combo_added)
    return list(cand)[:limit]


def unique_keep_order(seq: Iterable[str]) -> List[str]:
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def gen_variants(lemma: str) -> List[str]:
    seeds = list(gen_prefix_hyphen_variants(lemma))
    ocrs = []
    for s in seeds:
        ocrs.extend(gen_ocr_variants(s))
    allv = unique_keep_order([lemma] + seeds + ocrs)
    normies = []
    for s in allv:
        _n = norm_latin(s)
        if _n != s:
            normies.append(_n)
    allv = unique_keep_order(allv + normies)
    return allv[:200]


# =========================
# SQLite helpers (autodetect)
# =========================


def open_sqlite(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def autodetect_table_and_cols(
    conn: sqlite3.Connection,
    *,
    lemma_candidates=("lemma", "headword", "lema", "lema_canonico"),
    def_candidates=(
        "definition",
        "def",
        "def_fr",
        "def_pt",
        "gloss",
        "content",
        "texte",
        "text",
        "definicao",
    ),
    pos_candidates=("pos", "gram", "pos_tag", "classes", "itypes", "catgram"),
    id_candidates=("id", "entry_id", "doc_id"),
) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
    """
    Retorna (table, lemma_col, def_col|None, pos_col|None, id_col|None)
    """
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    for t in tables:
        cur.execute(f"PRAGMA table_info({t})")
        cols = [r[1].lower() for r in cur.fetchall()]
        lemma_col = next((c for c in lemma_candidates if c in cols), None)
        if lemma_col:
            def_col = next((c for c in def_candidates if c in cols), None)
            pos_col = next((c for c in pos_candidates if c in cols), None)
            id_col = next((c for c in id_candidates if c in cols), None)
            return (t, lemma_col, def_col, pos_col, id_col)
    raise RuntimeError("Não encontrei tabela/colunas de lemma no DB.")


# =========================
# Consultas LS (formato genérico, como Gaffiot)
# =========================


def query_dict_by_lemma(
    db_path: str, candidates: List[str], topn: int = 50
) -> List[Dict]:
    """
    Consulta dicionários 'genéricos' (LS novo, Gaffiot etc.) por lemma.
    Autodetecta tabela/colunas e aplica UNACCENT_LOWER.
    Retorna: [{lemma, lemma_norm, definition, pos}]
    """
    if not db_path or not candidates:
        return []
    conn = open_sqlite(db_path)
    conn.create_function("UNACCENT_LOWER", 1, unaccent_lower, deterministic=True)
    try:
        table, lemma_col, def_col, pos_col, _ = autodetect_table_and_cols(conn)
    except Exception:
        conn.close()
        return [{"_error": "autodetect failed"}]

    cand_norm = [norm_latin(c) for c in candidates]
    placeholders = ",".join("?" for _ in cand_norm)
    select_cols = f"{lemma_col} AS lemma"
    select_cols += f", {def_col} AS definition" if def_col else ", '' AS definition"
    select_cols += f", {pos_col} AS pos" if pos_col else ", '' AS pos"

    sql = f"""
      SELECT {select_cols}
      FROM {table}
      WHERE UNACCENT_LOWER({lemma_col}) IN ({placeholders})
      LIMIT {topn}
    """
    cur = conn.cursor()
    rows = []
    try:
        cur.execute(sql, tuple(cand_norm))
        rows = [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        # fallback menos robusto
        sql2 = f"""
          SELECT {select_cols}
          FROM {table}
          WHERE {lemma_col} IN ({placeholders})
          LIMIT {topn}
        """
        cur.execute(sql2, tuple(candidates))
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    out = []
    for r in rows:
        out.append(
            {
                "lemma": r.get("lemma"),
                "lemma_norm": norm_latin(r.get("lemma") or ""),
                "definition": r.get("definition"),
                "pos": r.get("pos"),
            }
        )
    return out


# =========================
# Lat→Deu
# =========================

LATDEU_GRAMMAR_TO_BUCKET = {"v": "VERB", "a": "ADJ", "s": "NOUN", "adv": "ADV"}


def query_latdeu_by_forms(
    latdeu_db: str, latins: List[str], topn: int = 100
) -> List[Dict]:
    if not latdeu_db or not latins:
        return []
    latins = sorted(set(norm_latin(l).strip() for l in latins if l and l.strip()))
    if not latins:
        return []
    conn = open_sqlite(latdeu_db)
    conn.create_function("UNACCENT_LOWER", 1, unaccent_lower, deterministic=True)
    cur = conn.cursor()
    placeholders = ",".join("?" for _ in latins)
    q = f"""
    SELECT
      F.form,
      F.bestimmung,
      F.form_norm,
      V.id      AS voc_id_seq,
      V.vok_id  AS vok_id,
      V.latin,
      V."desc"  AS desc,
      V.grammar AS grammar,
      V."key"   AS key,
      V.typnr   AS typnr
    FROM FORM AS F
    JOIN VOC  AS V ON V.vok_id = F.vok_id
    WHERE F.form_norm IN ({placeholders})
    ORDER BY V.id, F.form
    LIMIT {topn};
    """
    try:
        cur.execute(q, tuple(latins))
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        rows = [{"_error": f"latdeu query failed: {e}"}]
    finally:
        conn.close()
    return rows


def latdeu_buckets(rows: List[Dict]) -> List[str]:
    buckets = set()
    for r in rows or []:
        g = str(r.get("grammar") or "").strip().lower()
        if not g:
            continue
        if len(g) <= 3 and g in LATDEU_GRAMMAR_TO_BUCKET:
            buckets.add(LATDEU_GRAMMAR_TO_BUCKET[g])
        else:
            if "verb" in g:
                buckets.add("VERB")
            if "adj" in g:
                buckets.add("ADJ")
            if "subst" in g or "noun" in g:
                buckets.add("NOUN")
            if "adv" in g:
                buckets.add("ADV")
    return sorted(buckets)


# =========================
# Gaffiot (genérico) — reutiliza query_dict_by_lemma
# =========================

GAFFIOT_POS_MAP = {
    "v": "VERB",
    "verb": "VERB",
    "a": "ADJ",
    "adj": "ADJ",
    "adjectif": "ADJ",
    "s": "NOUN",
    "n": "NOUN",
    "nom": "NOUN",
    "subst": "NOUN",
    "adv": "ADV",
}


def gaffiot_buckets(rows: List[Dict]) -> List[str]:
    buckets = set()
    for r in rows or []:
        pos = str(r.get("pos") or "").strip().lower()
        if not pos:
            continue
        for tok, buck in GAFFIOT_POS_MAP.items():
            if tok in pos:
                buckets.add(buck)
    return sorted(buckets)


# =========================
# FAISS + Ollama
# =========================


def load_meta_jsonl(meta_path: str) -> List[Dict]:
    metas = []
    with open(meta_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                metas.append(json.loads(line))
    return metas


class OllamaClient:
    def __init__(self, base_url: str = "http://localhost:11434"):
        import requests

        self._requests = requests
        self.base_url = base_url.rstrip("/")

    def embed(
        self, texts: List[str], model: str, timeout: int = 300
    ) -> List[List[float]]:
        texts = [t if (t and t.strip()) else " " for t in texts]
        url = f"{self.base_url}/api/embed"
        payload = {"model": model, "input": texts}
        r = self._requests.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        embs = data.get("embeddings")
        if isinstance(embs, list) and embs:
            return embs
        emb1 = data.get("embedding")
        if isinstance(emb1, list) and emb1:
            return [emb1]
        raise RuntimeError(f"Resposta inesperada do Ollama: {data}")


def faiss_search(
    index_path: str,
    meta_path: str,
    query_text: str,
    model: str,
    k: int = 5,
    ollama_url: str = "http://localhost:11434",
) -> List[Dict]:
    import faiss, numpy as np

    metas = load_meta_jsonl(meta_path)
    client = OllamaClient(ollama_url)
    vec = client.embed([query_text], model=model)[0]
    index = faiss.read_index(index_path)
    q = np.array([vec], dtype="float32")
    D, I = index.search(q, k)
    out = []
    for score, pos in zip(D[0].tolist(), I[0].tolist()):
        if 0 <= pos < len(metas):
            m = dict(metas[pos])
            m["_score"] = float(score)
            m["_pos"] = int(pos)
            out.append(m)
    return out


# =========================
# Heurísticas morfológicas
# =========================


def ls_guess_morph(row: Dict) -> str:
    pos = str(row.get("pos") or "").lower()
    txt = str(row.get("definition") or "").lower()
    if "verb" in pos or re.search(r"\bv\b", pos):
        return "VERB"
    if "adv" in pos:
        return "ADV"
    if "adj" in pos:
        return "ADJ"
    if any(t in pos for t in ("subst", "noun", "s.")):
        return "NOUN"
    if "gen. pl" in txt or "ōrum" in txt or "orum" in txt:
        return "NOUN"
    if "people" in txt or "gentilic" in txt:
        return "NOUN"
    return "UNK"


def morph_bucket_from_text(s: str) -> str:
    s = (s or "").lower()
    if s.startswith(("s. ", "s.")):
        return "NOUN"
    if s.startswith(("adj", "adjet")):
        return "ADJ"
    if s.startswith(("v.", "v ")):
        return "VERB"
    if s.startswith(("adv",)):
        return "ADV"
    return "UNK"


GENT_TOKENS = {
    "people",
    "orum",
    "civitas",
    "urbs",
    "flumen",
    "gentilic",
    "populus",
    "povo",
    "cidade",
    "rio",
    "gentílico",
    "gentilício",
}


def is_gentilic(def_pt: str, def_ls: str) -> bool:
    blob = f"{def_pt or ''} {def_ls or ''}".lower()
    return any(tok in blob for tok in GENT_TOKENS)


# =========================
# Gates
# =========================


def is_affix(lemma: str) -> bool:
    return (lemma or "").startswith("-") or (lemma or "").endswith("-")


def affix_guard(lemma_in: str, lemma_out: str) -> bool:
    return (is_affix(lemma_in) == is_affix(lemma_out)) or (
        norm_latin(lemma_in) == norm_latin(lemma_out)
    )


def pass_char_similarity(lemma_in: str, lemma_out: str, conf: str) -> bool:
    r = ratio(lemma_in, lemma_out)
    thr = 0.35 if conf == "high" else 0.50
    return r >= thr


def within_alpha_span(suggested: str, neighbors: Dict) -> bool:
    prevs = neighbors.get("prev") or []
    nexts = neighbors.get("next") or []
    if not prevs or not nexts:
        return True

    def norm(s):
        return strip_accents(s).lower()

    left = norm(prevs[-1][1])
    right = norm(nexts[0][1])
    sug = norm(suggested)
    return left <= sug <= right


def morph_compatible(src_bucket: str, sugg_bucket: str, gent: bool) -> bool:
    if src_bucket == "UNK" or sugg_bucket == "UNK":
        return True
    if src_bucket == sugg_bucket:
        return True
    if gent and {src_bucket, sugg_bucket} == {"NOUN", "ADJ"}:
        return True
    return False


# =========================
# Sugestão principal
# =========================


def suggest_fix(
    lemma_in: str,
    context: Optional[str],
    ls_db_path: str,
    faiss_index: Optional[str],
    faiss_meta: Optional[str],
    latdeu_db: Optional[str],
    gaffiot_db: Optional[str],
    model: str,
    k: int = 5,
) -> Dict:
    variants = gen_variants(lemma_in)

    # 1) LS (genérico)
    ls_hits = query_dict_by_lemma(ls_db_path, variants, topn=50)

    best = None
    conf = "low"
    reason = []
    source = None

    if ls_hits and not any("_error" in h for h in ls_hits):
        scored = []
        for h in ls_hits:
            cand = h.get("lemma") or h.get("lemma_norm") or ""
            r = ratio(lemma_in, cand)
            ed = -edit_distance(norm_latin(lemma_in), norm_latin(cand))
            scored.append((r, ed, h))
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        best = scored[0][2]
        conf = "high"
        reason.append("match direto em LS (genérico) por lemma/lemma_norm")
        source = "LS"

    # 2) FAISS (se há contexto)
    faiss_hits = []
    if context and faiss_index and faiss_meta:
        try:
            faiss_hits = faiss_search(
                index_path=faiss_index,
                meta_path=faiss_meta,
                query_text=context,
                model=model,
                k=k,
            )
            if best is None and isinstance(faiss_hits, list) and faiss_hits:
                pool = faiss_hits[: min(3, len(faiss_hits))]
                scored = []
                for h in pool:
                    cand = h.get("lemma") or h.get("lemma_norm") or ""
                    sr = ratio(lemma_in, cand)
                    scored.append((sr, float(h.get("_score", 0.0)), h))
                scored.sort(key=lambda t: (t[0], t[1]), reverse=True)
                sr_best, _, top = scored[0]
                if sr_best >= 0.58:
                    best = {
                        "id": top.get("id"),
                        "lemma": top.get("lemma"),
                        "lemma_norm": top.get("lemma_norm"),
                        "definition": top.get("definition"),
                        "pos": top.get("pos"),
                    }
                    reason.append("FAISS (definição) + reranking por forma")
                    conf = "med" if sr_best < 0.85 else "high"
                    source = "FAISS"
        except Exception as e:
            faiss_hits = [{"_error": f"faiss/ollama failed: {e}"}]

    # 3) Lat→Deu
    latdeu_hits_raw = query_latdeu_by_forms(latdeu_db, variants) if latdeu_db else []
    latdeu_buck = latdeu_buckets(latdeu_hits_raw)

    latdeu_support = []
    if latdeu_hits_raw and best:
        best_lem = norm_latin(best.get("lemma") or best.get("lemma_norm") or "")
        for row in latdeu_hits_raw:
            if "_error" in row:
                continue
            form = norm_latin(row.get("form", ""))
            latin_head = norm_latin(row.get("latin", ""))
            if form == best_lem or latin_head == best_lem:
                latdeu_support.append(row)
        if latdeu_support:
            reason.append("comprovado por Latin→German")
            if conf == "med":
                conf = "high"

    # 4) Gaffiot (genérico)
    gaff_hits_raw = (
        query_dict_by_lemma(gaffiot_db, variants, topn=50) if gaffiot_db else []
    )
    gaff_buck = gaffiot_buckets(gaff_hits_raw)

    if best is None and gaff_hits_raw and not any("_error" in h for h in gaff_hits_raw):
        scored = []
        for h in gaff_hits_raw:
            cand = h.get("lemma") or h.get("lemma_norm") or ""
            r = ratio(lemma_in, cand)
            ed = -edit_distance(norm_latin(lemma_in), norm_latin(cand))
            scored.append((r, ed, h))
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        top = scored[0][2]
        best = {
            "id": None,
            "lemma": top.get("lemma"),
            "lemma_norm": top.get("lemma_norm"),
            "definition": top.get("definition"),
            "pos": top.get("pos"),
        }
        reason.append("match direto em Gaffiot (genérico)")
        conf = "med"
        source = "GAFFIOT"

    suggestion = None
    ls_morph = None
    if best:
        suggestion = {
            "lemma_sugerido": best.get("lemma") or best.get("lemma_norm"),
            "motivo": "; ".join(reason),
            "confianca": conf,
            "source": source,
        }
        # classe morfológica preferindo a origem
        pos_src = (best.get("pos") or "").lower()
        if "ls" in (source or "").lower():
            ls_morph = ls_guess_morph(best)
        elif pos_src:
            for tok, buck in GAFFIOT_POS_MAP.items():
                if tok in pos_src:
                    ls_morph = buck
                    break

    return {
        "lemma_in": lemma_in,
        "context": context,
        "variants": variants[:25],
        "ls_hits": ls_hits[:10],
        "faiss_top": faiss_hits[:k] if isinstance(faiss_hits, list) else faiss_hits,
        "latdeu_hits": (
            latdeu_support[:10]
            if latdeu_support
            else (latdeu_hits_raw[:5] if latdeu_hits_raw else [])
        ),
        "latdeu_buckets": latdeu_buck,
        "gaffiot_hits": gaff_hits_raw[:10] if gaff_hits_raw else [],
        "gaffiot_buckets": gaff_buck,
        "suggestion": suggestion,
        "ls_morph_class": ls_morph,
    }


# =========================
# retificado_v2 helpers (autodetect)
# =========================


def r_detect(conn: sqlite3.Connection):
    """
    Detecta tabela e colunas do retificado:
      id_col, lemma_col (lema_canonico|lemma), def_col (definicao|definition),
      morph_col (morfologia|morph), conf_col (conf), notes_col (notas), review_col (needs_review)
    """
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    pref = {
        "id": ("id", "entry_id"),
        "lemma": ("lema_canonico", "lemma", "headword"),
        "def": ("definicao", "definition", "def_pt"),
        "morph": ("morfologia", "morph"),
        "conf": ("conf",),
        "notes": ("notas", "notes"),
        "review": ("needs_review", "review", "flag_review"),
    }
    for t in tables:
        cur.execute(f"PRAGMA table_info({t})")
        cols = {r[1].lower(): r[1] for r in cur.fetchall()}  # map lower->exact
        try_id = next((cols[c] for c in pref["id"] if c in cols), None)
        try_lemma = next((cols[c] for c in pref["lemma"] if c in cols), None)
        if try_id and try_lemma:
            try_def = next((cols[c] for c in pref["def"] if c in cols), None)
            try_morph = next((cols[c] for c in pref["morph"] if c in cols), None)
            try_conf = next((cols[c] for c in pref["conf"] if c in cols), None)
            try_notes = next((cols[c] for c in pref["notes"] if c in cols), None)
            try_rev = next((cols[c] for c in pref["review"] if c in cols), None)
            return {
                "table": t,
                "id": try_id,
                "lemma": try_lemma,
                "def": try_def,
                "morph": try_morph,
                "conf": try_conf,
                "notes": try_notes,
                "review": try_rev,
            }
    raise RuntimeError("Não foi possível autodetectar o esquema do retificado_v2.")


def get_neighbors(
    conn: sqlite3.Connection, meta: dict, entry_id: str, n: int = 2, sort="alpha"
) -> Dict:
    cur = conn.cursor()
    T = meta["table"]
    ID = meta["id"]
    LEM = meta["lemma"]
    cur.execute(f"SELECT {ID}, {LEM} FROM {T}")
    rows = [(r[0], r[1]) for r in cur.fetchall()]
    if sort == "alpha":
        rows.sort(key=lambda t: strip_accents(t[1]).lower())
    else:
        rows.sort(key=lambda t: str(t[0]))
    idx = next(
        (i for i, (rid, _) in enumerate(rows) if str(rid) == str(entry_id)), None
    )
    if idx is None:
        return {"prev": [], "next": []}
    prevs = rows[max(0, idx - n) : idx]
    nexts = rows[idx + 1 : idx + 1 + n]
    return {"prev": prevs, "next": nexts}


# =========================
# Entrada/seleção de suspeitos
# =========================


def load_suspects_from_db(
    conn: sqlite3.Connection, meta: dict, conf_filter: str
) -> List[Tuple[str, str, Optional[str]]]:
    T = meta["table"]
    ID = meta["id"]
    LEM = meta["lemma"]
    DEF = meta["def"]
    CONF = meta["conf"]
    cur = conn.cursor()
    if not CONF:
        return []
    like = f"%conf:{conf_filter}%"
    cur.execute(
        f"SELECT {ID}, {LEM}, {DEF or 'NULL'} FROM {T} WHERE {CONF} LIKE ?", (like,)
    )
    return [(str(r[0]), r[1], r[2]) for r in cur.fetchall()]


def load_items_from_tsv(path: str) -> List[Tuple[Optional[str], str, Optional[str]]]:
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 3:
                entry_id, lemma, ctx = (
                    parts[0].strip(),
                    parts[1].strip(),
                    "\t".join(parts[2:]).strip(),
                )
                items.append((entry_id, lemma, ctx))
            elif len(parts) == 2:
                lemma, ctx = parts[0].strip(), parts[1].strip()
                items.append((None, lemma, ctx))
            else:
                items.append((None, line.strip(), None))
    return items


# =========================
# Aplicação
# =========================


def backup_db(path: str) -> str:
    os.makedirs("backups", exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    base = os.path.basename(path)
    dest = os.path.join("backups", f"{os.path.splitext(base)[0]}.{ts}.db")
    shutil.copy2(path, dest)
    print(f"[info] backup criado: {dest}")
    return dest


def apply_update(
    conn: sqlite3.Connection,
    meta: dict,
    entry_id: str,
    old_lemma: str,
    new_lemma: str,
    motive: str,
    conf_set: str,
):
    T = meta["table"]
    ID = meta["id"]
    LEM = meta["lemma"]
    NOTES = meta["notes"]
    CONF = meta["conf"]
    cur = conn.cursor()
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    notas_expr = ""
    if NOTES:
        notas_expr = f", {NOTES} = COALESCE({NOTES}, '') || ?"
    sql = f"""UPDATE {T}
              SET {LEM} = ? {notas_expr}
              WHERE {ID} = ?"""
    params = [new_lemma]
    if NOTES:
        params.append(
            f" | [ocr_fix {stamp}] {motive} | lemma_in='{old_lemma}' → '{new_lemma}'"
        )
    params.append(entry_id)
    cur.execute(sql, tuple(params))

    if CONF:
        # não sobrescreve conf:high
        cur.execute(
            f"UPDATE {T} SET {CONF}=? WHERE {ID}=? AND ({CONF} IS NULL OR {CONF}!='conf:high')",
            (conf_set, entry_id),
        )


def mark_review(conn: sqlite3.Connection, meta: dict, entry_id: str):
    T = meta["table"]
    ID = meta["id"]
    REV = meta["review"]
    if not REV:
        return
    cur = conn.cursor()
    cur.execute(f"UPDATE {T} SET {REV}=1 WHERE {ID}=?", (entry_id,))


def load_all_from_db(conn: sqlite3.Connection, meta: dict) -> List[sqlite3.Row]:
    T = meta["table"]
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    cur.execute(f"SELECT * FROM {T}")
    return cur.fetchall()


def load_all_single_items_from_db(
    conn: sqlite3.Connection, meta: dict
) -> List[sqlite3.Row]:
    T = meta["table"]
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row

    # Seleciona apenas os itens que aparecem uma única vez (não duplicados)
    cur.execute(
        f"""
            SELECT *
            FROM {T}
            WHERE (
                SELECT COUNT(*)
                FROM {T} AS t2
                WHERE t2.lemma = {T}.lemma
            ) = 1
        """
    )
    return cur.fetchall()


# =========================
# CLI
# =========================


def parse_args():
    ap = argparse.ArgumentParser(
        description="Pipeline OCR fix com FAISS novo + LS/Gaffiot genéricos + Lat→Deu e retificado_v2 autodetect."
    )
    ap.add_argument("--retificado-db", required=True, help="retificado_v2.db (SQLite)")
    ap.add_argument(
        "--ls-db", required=True, help="LS (SQLite) no formato genérico (autodetect)"
    )
    ap.add_argument("--latdeu-db", help="token_latim_german.sqlite (opcional)")
    ap.add_argument("--gaffiot-db", help="gaffiot.db (opcional)")
    ap.add_argument("--faiss-index", help="index.faiss (opcional)")
    ap.add_argument("--faiss-meta", help="meta.jsonl (opcional)")
    ap.add_argument(
        "--model", default="nomic-embed-text", help="Modelo de embeddings (Ollama)"
    )
    ap.add_argument("--k", type=int, default=5, help="Top-k FAISS (default 5)")
    ap.add_argument(
        "--neighbors", type=int, default=2, help="Janela de vizinhos (default 2)"
    )
    ap.add_argument(
        "--neighbors-sort",
        choices=["alpha", "id"],
        default="alpha",
        help="Ordenação de vizinhos",
    )
    ap.add_argument(
        "--min-conf",
        choices=["low", "med", "high"],
        default="low",
        help="Conf mínima (não usada para filtrar dry-run)",
    )
    ap.add_argument(
        "--auto-suspects",
        choices=["low", "med", "high"],
        help="Seleciona suspeitos por conf no retificado",
    )
    ap.add_argument("--infile", help="TSV: id<TAB>lemma<TAB>contexto (ou sem id)")
    ap.add_argument(
        "--apply", action="store_true", help="Aplicar atualizações (default: dry-run)"
    )
    ap.add_argument(
        "--apply-mode",
        choices=["conservative", "review-only", "force"],
        default="conservative",
        help="conservative (segura), review-only (marca), force (aplica se >= min-conf)",
    )
    return ap.parse_args()


def extrair_terminacao_decl(decl_class: Optional[str]) -> Optional[str]:
    # "número (terminação)" da declinação, ex: "1ª (-ae)", "2ª (-i)", "3ª (-is)", "4ª (-us)", "5ª (-ei)"
    # Retornar a terminação sem o hífen, ex: "ae", "i", "is", "us", "ei"
    if not decl_class:
        return None
    m = re.search(r"\((-[a-zA-Z]+)\)", decl_class)
    if m:
        return m.group(1).lstrip("-").lower()
    return None


# Genetivus	-ārum 	-ōrum 		-um		-uum 		-ērum
def genitivus_to_plural(terminacao: Optional[str]) -> Optional[str]:
    """
    Converte a terminação do genitivo singular para plural.
    Exemplo: "-ae" → "-ārum", "-i" → "-ōrum", "-is" → "-um", "-us" → "-uum", "-ei" → "-ērum"
    """
    if not terminacao:
        return None
    mapping = {
        "ae": "ārum",
        "i": "ōrum",
        "is": "um",
        "us": "uum",
        "ei": "ērum",
    }
    return mapping.get(terminacao, None)


def conjugacao_para_numero(terminacao: Optional[str]) -> Optional[str]:
    """Converte a terminação de conjugação para número.
    amō, -āre
    habeō, -ēre
    dīcō, -ere
    audiō, -īre

    āre -> 1
    ēre -> 2
    ere -> 3
    īre -> 4

    """

    if not terminacao:
        return terminacao
    mapping = {
        "āre": "1",
        "are": "1",
        "ēre": "2",
        "ere": "3",
        "īre": "4",
    }
    return mapping.get(terminacao.strip(), terminacao)


def get_filtered_items_from_db(ls_entry: sqlite3.Row):
    lemma_ls = unaccent_lower(ls_entry["lemma"])
    gender_ls = remove_symbols(unaccent_lower(ls_entry["gen_text"]))
    itypesstr = unaccent_lower(ls_entry["itype"])
    itypes_ls = itypesstr.split(",") if itypesstr else []

    def unaccent_strip(s: Optional[str]) -> Optional[str]:
        if s is None:
            return s
        return unaccent_lower(conjugacao_para_numero(s)).strip()

    itypes_ls = [v for v in set(map(unaccent_strip, itypes_ls)) if v]

    try:
        itypes_json = json.loads(ls_entry["itype_json"])

        itypes_sum = []

        for itype in itypes_json:
            if isinstance(itype, str):
                itypes_cur = itype.split(",") if itype else []

                itypes_sum += [
                    unaccent_lower(conjugacao_para_numero(v)).strip()
                    for v in itypes_cur
                ]

        itypes_ls = list(set(itypes_ls + itypes_sum))
        itypes_ls = [v for v in set(map(unaccent_strip, itypes_ls)) if v]
    except (TypeError, json.JSONDecodeError):
        itypes_json = None

    if gender_ls is not None:
        gender_ls = unaccent_lower(gender_ls).replace(".", "")

    pos_ls = unaccent_lower(ls_entry["pos"])

    if pos_ls is not None:
        pos_ls = pos_ls.replace(".", "")

    return (lemma_ls, gender_ls, itypes_ls, pos_ls)


def main():
    args = parse_args()

    rconn = open_sqlite(args.retificado_db)
    rconn.create_function("UNACCENT_LOWER", 1, unaccent_lower, deterministic=True)
    rmeta = r_detect(rconn)

    lsdict = open_sqlite(args.ls_db)
    lsdict.create_function("UNACCENT_LOWER", 1, unaccent_lower, deterministic=True)
    metals = autodetect_table_and_cols(lsdict)

    # Gaffiot
    gaffiot_db = args.gaffiot_db if args.gaffiot_db else None
    if gaffiot_db:
        gaffiot_conn = open_sqlite(gaffiot_db)
        gaffiot_conn.create_function(
            "UNACCENT_LOWER", 1, unaccent_lower, deterministic=True
        )
    else:
        gaffiot_conn = None

    items = load_all_single_items_from_db(rconn, rmeta)
    itemsls = load_all_single_items_from_db(lsdict, {"table": "entry"})
    itemsgaf = (
        load_all_single_items_from_db(gaffiot_conn, {"table": "entry"})
        if gaffiot_conn
        else []
    )

    print(f"[info] {len(items)} itens únicos encontrados no retificado_v2.")
    print(f"[info] {len(itemsls)} itens únicos encontrados no LS.")

    # Printar os nomes das colunas autodetectadas
    print(f"[info] Tabela retificado_v2: {items[0].keys()}")
    print(f"[info] Colunas LS: {itemsls[0].keys()}")

    # Fazer um dict relacionando lemma norm para fácil acesso do LS
    ls_lemmas = {unaccent_lower(r["lemma"]): r for r in itemsls}
    gaffiot_lemmas = {unaccent_lower(r["lemma"]): r for r in itemsgaf}

    # Backup se vamos aplicar
    if args.apply:
        backup_db(args.retificado_db)

    to_apply = []
    motive = "Correção automática de lema via LS"

    for item in items:
        lemma_sort_retificado = unaccent_lower(item[rmeta["lemma"]])

        if lemma_sort_retificado not in ls_lemmas:
            continue

        ls_entry = ls_lemmas[lemma_sort_retificado]
        if lemma_sort_retificado in gaffiot_lemmas:
            gaffiot_entry = gaffiot_lemmas[lemma_sort_retificado]
        else:
            continue
        lemma_original = ls_entry["lemma"]
        lemma_gaffiot = gaffiot_entry["lemma"]

        if lemma_original is None or not lemma_original.strip():
            print(f"[info] Lema original vazio para {lemma_sort_retificado}. Pulando.")
            continue

        if lemma_gaffiot is None or not lemma_gaffiot.strip():
            print(f"[info] Lema Gaffiot vazio para {lemma_sort_retificado}. Pulando.")
            continue

        gender = unaccent_lower(item["gender"])

        if gender is not None:
            gender = gender.replace(".", "")

        pos_retificado = unaccent_lower(item["pos"])
        decl_class = unaccent_lower(item["decl_class"])

        decl_terminacao = extrair_terminacao_decl(decl_class)
        gen_plural = genitivus_to_plural(decl_terminacao)

        if pos_retificado is not None:
            pos_retificado = pos_retificado.replace(".", "")

        lemma_ls, gender_ls, itypes_ls, pos_ls = get_filtered_items_from_db(ls_entry)
        lemma_gaf, gendef_gaf, itypes_gaf, pos_gaf = get_filtered_items_from_db(
            gaffiot_entry
        )

        if not lemma_ls or not lemma_gaf:
            print(
                f"[info] Lema LS ou Gaffiot vazio para {lemma_sort_retificado}. Pulando."
            )
            continue

        if gender_ls != gendef_gaf:
            print(
                f"[info] Gênero diferente para {lemma_sort_retificado}: {gendef_gaf} (Gaffiot) vs {gender_ls} (LS)"
            )
            continue

        if pos_gaf is not None and pos_ls is not None and pos_gaf != pos_ls:
            print(
                f"[info] Classe morfológica diferente para {lemma_sort_retificado}: {pos_gaf} (Gaffiot) vs {pos_ls} (LS)"
            )
            continue

        if itypes_gaf != itypes_ls:
            print(dict(gaffiot_entry))
            print(
                f"[info] Tipos de declinação diferentes para {lemma_sort_retificado}: {itypes_gaf} (Gaffiot) vs {itypes_ls} (LS)"
            )
            continue

        print(f"[info] Verificando {lemma_sort_retificado}...")
        lemma_original_retificado = item["lemma"]

        to_apply.append(
            {
                "id": item["id"],
                "lemma": lemma_original_retificado,
                "lemma_sort": unaccent_lower(lemma_original_retificado),
                "old_lemma": lemma_original_retificado,
                "gender": gender_ls or gendef_gaf or gender,
                "gender_before": gender,
                "pos": pos_retificado or ls_entry["pos"] or gaffiot_entry["pos"],
                "pos_before": pos_retificado,
                "itypes": ls_entry["itype"] or gaffiot_entry["itype"] or decl_class,
                "itypes_before": decl_class,
                "motive": motive,
            }
        )
        continue
        # if gender_ls != gender:
        #     print(
        #         f"[info] Gênero diferente para {lemma_sort_retificado}: {gender} (retificado) vs {gender_ls} (LS)"
        #     )

        if (
            decl_terminacao not in itypes_ls
            and unaccent_lower(gen_plural) not in itypes_ls
        ):
            print(
                f"[info] Declinação diferente para {lemma_sort_retificado}: {decl_terminacao} (retificado) {unaccent_lower(gen_plural)} (plural) vs {itypes_ls} (LS)"
            )
            continue

        # averiguar necessidade de correção do lema
        lemma_original_retificado = item["lemma"]
        lemma_original_ls = lemma_original

        print(f"{lemma_original_retificado}  -> {lemma_original_ls}")

        if lemma_original_retificado != norm_latin_fusions(lemma_original_ls):
            print(
                f"[info] Corrigindo lema de {lemma_original_retificado} para {lemma_original_ls}"
            )

            to_apply.append(
                {
                    "id": item["id"],
                    "lemma": norm_latin_fusions(lemma_original_ls),
                    "lemma_sort": unaccent_lower(lemma_original_ls),
                    "old_lemma": lemma_original_retificado,
                    "motive": motive,
                }
            )

    cur = rconn.cursor()
    T = rmeta["table"]
    ID = rmeta["id"]
    LEM = rmeta["lemma"]
    DEF = rmeta["def"]
    MOR = rmeta["morph"]

    print("\n\n\n\n")

    if args.apply:
        for entry in to_apply:
            print(
                f"[info] Aplicando correção para id={entry['id']}: {entry['old_lemma']} -> {entry['lemma']}"
            )
            meta_copy = rmeta.copy()
            meta_copy["id"] = "id"

            apply_update(
                rconn,
                meta_copy,
                entry_id=str(entry["id"]),
                old_lemma=entry["old_lemma"],
                new_lemma=entry["lemma"],
                motive=entry["motive"],
                conf_set="conf:high",
            )

        rconn.commit()
    else:
        for entry in to_apply:
            print(
                f"[info] DRY: correção para id={entry['id']}: {entry['old_lemma']} -> {entry['lemma']}; gender={entry['gender_before']} -> {entry['gender']}; pos={entry['pos_before']} -> {entry['pos']}; itypes={entry['itypes_before']} -> {entry['itypes']}"
            )
        rconn.rollback()

    rconn.close()


if __name__ == "__main__":
    main()
