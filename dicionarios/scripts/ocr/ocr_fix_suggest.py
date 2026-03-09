#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ocr_fix_suggest.py — com guard-rails morfológicos (LS itypes/pos) e vizinhança alpha opcional

Diferenças principais:
- Mantém suporte a TSV 3 colunas (id, lemma, contexto).
- Lê pos/itypes do ls_dict.sqlite e estima uma 'ls_morph_class'.
- Compara com a morfologia do retificado.db (se fornecido via --retificado-db).
- Abaixa confiança e marca 'guard_morph_conflict' em colisões (p.ex. Ubi conj. × Ubii povo).
- Opcional: inclui vizinhos alfabéticos do retificado.db no JSON (para auditoria humana).

Uso:
  python3 ocr_fix_suggest.py \
    --infile suspeitos.tsv \
    --ls-db ls_dict.sqlite \
    --faiss-index index_ls/index.faiss \
    --faiss-meta index_ls/meta.jsonl \
    --latdeu-db latdeu.sqlite \
    --retificado-db retificado.db \
    --model nomic-embed-text
"""
import argparse, json, sqlite3, unicodedata, re
from typing import List, Dict, Tuple, Iterable, Optional

# ---------- texto ----------
def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def norm_latin(s: str) -> str:
    x = strip_accents(s or "").lower()
    x = x.replace("j", "i").replace("v", "u").replace("ſ","s")
    x = re.sub(r"[\^\·\.\(\)\[\]\{\}°˘̆̄¯´`ʹ]", "", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

PREFIXES = ["ab","ad","con","de","dis","ex","in","inter","ob","per","post","prae","pro","re","sub","super","trans"]
OCR_PAIRS = [("o","u"),("u","o"),("rn","m"),("m","rn"),("cl","d"),("d","cl"),("æ","ae"),("œ","oe"),("j","i"),("v","u")]

def gen_prefix_hyphen_variants(lemma: str) -> Iterable[str]:
    y_norm = norm_latin(lemma)
    yield lemma
    for p in PREFIXES:
        if y_norm.startswith(p) and len(y_norm) > len(p):
            yield lemma[:len(p)] + "-" + lemma[len(p):]
            if len(lemma) > len(p)+1 and lemma[len(p)] == "-":
                yield lemma.replace("-", "", 1)

def gen_ocr_variants(lemma: str, limit: int = 100) -> List[str]:
    cand = {lemma}
    for a,b in OCR_PAIRS:
        for base in list(cand):
            if a in base:
                cand.add(base.replace(a,b))
    # 2 trocas combinadas (limite conservador)
    base_list = list(cand)
    for i,(a1,b1) in enumerate(OCR_PAIRS):
        for a2,b2 in OCR_PAIRS[i+1:]:
            for s in base_list:
                t = s.replace(a1,b1)
                u = t.replace(a2,b2)
                if u != s:
                    cand.add(u)
                if len(cand) >= limit:
                    break
            if len(cand) >= limit:
                break
        if len(cand) >= limit:
            break
    # normalizados
    normies = [norm_latin(s) for s in list(cand)]
    cand.update(normies)
    return list(cand)[:200]

def unique_keep_order(seq: Iterable[str]) -> List[str]:
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            out.append(x); seen.add(x)
    return out

def gen_variants(lemma: str) -> List[str]:
    seeds = list(gen_prefix_hyphen_variants(lemma))
    ocrs  = []
    for s in seeds:
        ocrs.extend(gen_ocr_variants(s))
    allv = unique_keep_order([lemma] + seeds + ocrs)
    return allv[:200]

def ratio(a: str, b: str) -> float:
    a = a or ""; b = b or ""
    if not a and not b: return 1.0
    A = set([a[i:i+2] for i in range(max(1,len(a)-1))])
    B = set([b[i:i+2] for i in range(max(1,len(b)-1))])
    if not A and not B: return 1.0
    inter = len(A & B); uni = len(A | B) or 1
    return inter / uni

def edit_distance(a: str, b: str) -> int:
    a = a or ""; b = b or ""
    la, lb = len(a), len(b)
    dp = list(range(lb+1))
    for i in range(1, la+1):
        prev = dp[0]; dp[0] = i; ca = a[i-1]
        for j in range(1, lb+1):
            temp = dp[j]
            cost = 0 if ca == b[j-1] else 1
            dp[j] = min(dp[j] + 1, dp[j-1] + 1, prev + cost)
            prev = temp
    return dp[lb]

# ---------- DB helpers ----------
def open_sqlite(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn

def query_ls_by_lemma(conn: sqlite3.Connection, candidates: List[str], topn: int = 20) -> List[Dict]:
    if not candidates: return []
    cand_norm = [norm_latin(c) for c in candidates]
    placeholders = ",".join("?" for _ in cand_norm)
    sql = f"""
      SELECT id, lemma, lemma_norm, pos, itypes, definition, tr_gloss_pt, tr_trad_pt
      FROM ls_entries
      WHERE lemma_norm IN ({placeholders}) OR lemma IN ({placeholders})
      LIMIT {topn}
    """
    cur = conn.cursor()
    cur.execute(sql, tuple(cand_norm + candidates))
    return [dict(r) for r in cur.fetchall()]

def query_latdeu_by_forms(latdeu_db: str, latins: List[str]) -> List[Dict]:
    if not latdeu_db or not latins: return []
    latins = sorted(set([norm_latin(l).strip() for l in latins if l.strip()]))
    if not latins: return []
    conn = open_sqlite(latdeu_db)
    cur = conn.cursor()
    placeholders = ",".join("?" for _ in latins)
    q = f"""
    SELECT F.form, V.latin, V."desc" AS desc, V.grammar AS grammar_note, V.id
    FROM FORM AS F
    JOIN VOC  AS V ON V.vok_id = F.vok_id
    WHERE F.form_norm IN ({placeholders})
    ORDER BY V.id, F.form;
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

# ---------- FAISS + Ollama ----------
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
    def embed(self, texts: List[str], model: str, timeout: int = 300) -> List[List[float]]:
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

def faiss_search(index_path: str, meta_path: str, query_text: str, model: str, k: int = 5,
                 ollama_url: str = "http://localhost:11434") -> List[Dict]:
    import faiss, numpy as np
    metas = load_meta_jsonl(meta_path)
    vec = OllamaClient(ollama_url).embed([query_text], model=model)[0]
    index = faiss.read_index(index_path)
    q = np.array([vec], dtype="float32")
    D, I = index.search(q, k)
    out = []
    for score, pos in zip(D[0].tolist(), I[0].tolist()):
        if 0 <= pos < len(metas):
            m = dict(metas[pos]); m["_score"] = float(score); m["_pos"] = int(pos)
            out.append(m)
    return out

# ---------- Guard-rails morfológicos ----------
def ls_guess_morph(row: Dict) -> str:
    """
    Heurística grobe de classe morfológica a partir de itypes/pos.
      - contém 'ōrum' => 2ª decl. masc. plural (etnônimo/povo)
      - contém 'a, um' => adjetivo 1/2
      - 'people' / 'city' em definition => próprio/coletivo
      - pos vazio é normal em LS; usar pistas
    """
    it = (row.get("itypes") or "").lower()
    df = (row.get("definition") or "").lower()
    if "ōrum" in it or "orum" in it:
        return "noun_2nd_pl"
    if "a, um" in it:
        return "adj_1_2"
    if any(k in df for k in ["people","tribe","nation","city","river"]):
        return "proper_or_collective"
    return "unknown"

def morph_class_from_entry(morfologia: str) -> str:
    m = (morfologia or "").lower()
    if "conj" in m or "conjun" in m: return "function_conj"
    if "adv" in m: return "function_adv"
    if "pron" in m: return "function_pron"
    if "indecl" in m: return "noun_indecl"
    if "s." in m: return "noun"
    return "unknown"

def morph_conflicts(ls_class: str, entry_class: str) -> bool:
    # Conflito “duro”: LS indica substantivo/plural/coletivo vs entrada marcada como conj/adv/pron
    hard = (ls_class in {"noun_2nd_pl","proper_or_collective","adj_1_2"} and
            entry_class in {"function_conj","function_adv","function_pron"})
    return hard

# ---------- Vizinhança alfabética (opcional) ----------
def alpha_neighbors(cur_ret: sqlite3.Cursor, entry_id: str, w: int = 5) -> Dict[str, List[str]]:
    q = """
    WITH ranked AS (
      SELECT id, lema_canonico, ROW_NUMBER() OVER (ORDER BY lema_canonico_norm) rn
      FROM entries
    ),
    t AS (SELECT rn FROM ranked WHERE id = ?)
    SELECT id, lema_canonico FROM ranked
    WHERE rn BETWEEN (SELECT rn FROM t)-? AND (SELECT rn FROM t)+?
    ORDER BY rn;
    """
    cur_ret.execute(q, (entry_id, w, w))
    ids, lems = [], []
    for r in cur_ret.fetchall():
        ids.append(r[0]); lems.append(r[1])
    # se o id está dentro, separar prev/next
    out = {"prev": [], "next": []}
    if entry_id in ids:
        i = ids.index(entry_id)
        out["prev"] = lems[:i]
        out["next"] = lems[i+1:]
    return out

# ---------- core ----------
def suggest_fix(lemma_in: str,
                context: Optional[str],
                ls_conn: sqlite3.Connection,
                faiss_index: Optional[str],
                faiss_meta: Optional[str],
                latdeu_db: Optional[str],
                model: str,
                k: int = 5) -> Dict:
    variants = gen_variants(lemma_in)
    ls_hits = query_ls_by_lemma(ls_conn, variants, topn=50)

    faiss_hits = []
    if context and faiss_index and faiss_meta:
        try:
            faiss_hits = faiss_search(faiss_index, faiss_meta, context, model, k)
        except Exception as e:
            faiss_hits = [{"_error": f"faiss/ollama failed: {e}"}]

    latdeu_hits = query_latdeu_by_forms(latdeu_db, variants) if latdeu_db else []

    best = None
    reason = []
    conf = "low"

    if ls_hits:
        scored = []
        for h in ls_hits:
            r = ratio(norm_latin(lemma_in), norm_latin(h.get("lemma") or h.get("lemma_norm") or ""))
            ed = -edit_distance(norm_latin(lemma_in), norm_latin(h.get("lemma") or ""))
            scored.append((r, ed, h))
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        best = scored[0][2]
        conf = "high"
        reason.append("match direto em LS por lemma/lemma_norm")

    if best is None and isinstance(faiss_hits, list) and faiss_hits and "_score" in faiss_hits[0]:
        top = faiss_hits[0]
        best = {"id": top.get("id"), "lemma": top.get("lemma"), "lemma_norm": top.get("lemma_norm"),
                "pos": top.get("pos"), "itypes": top.get("itypes")}
        reason.append("similaridade semântica FAISS (definição/gloss)")
        sr = ratio(norm_latin(lemma_in), norm_latin(top.get("lemma","") or top.get("lemma_norm","")))
        conf = "med" if sr < 0.85 else "high"

    # Latin→German como “reforço”
    latdeu_support = []
    if latdeu_hits and best:
        best_lem = norm_latin(best.get("lemma") or best.get("lemma_norm") or "")
        for row in latdeu_hits:
            if "_error" in row: continue
            form = norm_latin(row.get("form",""))
            latin_head = norm_latin(row.get("latin",""))
            if form == best_lem or latin_head == best_lem:
                latdeu_support.append(row)
        if latdeu_support:
            reason.append("comprovado por Latin→German")
            if conf == "med": conf = "high"

    suggestion = None
    if best:
        suggestion = {
            "lemma_sugerido": best.get("lemma") or best.get("lemma_norm"),
            "motivo": "; ".join(reason),
            "confianca": conf,
            "ls_pos": best.get("pos"),
            "ls_itypes": best.get("itypes"),
            "ls_morph_class": ls_guess_morph(best if "itypes" in best else {"itypes": best.get("itypes",""), "definition": ""}),
        }

    return {
        "lemma_in": lemma_in,
        "context": context,
        "variants": variants[:25],
        "ls_hits": ls_hits[:10],
        "faiss_top": faiss_hits[:k] if isinstance(faiss_hits, list) else faiss_hits,
        "latdeu_hits": latdeu_support[:10] if latdeu_support else (latdeu_hits[:5] if latdeu_hits else []),
        "suggestion": suggestion
    }

# ---------- CLI ----------
def parse_args():
    ap = argparse.ArgumentParser(description="Sugere correções para lemas suspeitos (LS + FAISS + Latin→German) com guard-rails morfológicos.")
    g_in = ap.add_mutually_exclusive_group(required=True)
    g_in.add_argument("--lemma", help="Lemma único para sugerir correção.")
    g_in.add_argument("--infile", help="TSV: id<TAB>lemma<TAB>contexto (ou formato antigo).")
    ap.add_argument("--context", help="Contexto opcional para --lemma (usado em FAISS).")
    ap.add_argument("--ls-db", required=True, help="ls_dict.sqlite")
    ap.add_argument("--faiss-index", help="index.faiss (opcional)")
    ap.add_argument("--faiss-meta", help="meta.jsonl (opcional)")
    ap.add_argument("--latdeu-db", help="DB Latin→German (opcional)")
    ap.add_argument("--retificado-db", help="retificado.db — para checar morfologia atual e vizinhança alpha (opcional)")
    ap.add_argument("--model", default="nomic-embed-text", help="Modelo de embeddings (Ollama)")
    ap.add_argument("--k", type=int, default=5, help="Top-k FAISS")
    ap.add_argument("--alpha-window", type=int, default=5, help="Janela de vizinhança alpha se --retificado-db for usado")
    return ap.parse_args()

def main():
    args = parse_args()
    ls_conn = open_sqlite(args.ls_db)

    ret_conn = None
    cur_ret  = None
    if args.retificado_db:
        ret_conn = open_sqlite(args.retificado_db)
        cur_ret  = ret_conn.cursor()

    items: List[Tuple[Optional[str], str, Optional[str]]] = []
    if args.lemma:
        items.append((None, args.lemma.strip(), args.context.strip() if args.context else None))
    else:
        with open(args.infile, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip(): continue
                parts = line.rstrip("\n").split("\t")
                if len(parts) >= 3:
                    entry_id, lemma, ctx = parts[0].strip(), parts[1].strip(), "\t".join(parts[2:]).strip()
                    items.append((entry_id, lemma, ctx))
                elif len(parts) == 2:
                    lemma, ctx = parts[0].strip(), parts[1].strip()
                    items.append((None, lemma, ctx))
                else:
                    items.append((None, line.strip(), None))

    for entry_id, lemma, ctx in items:
        res = suggest_fix(
            lemma_in=lemma,
            context=ctx,
            ls_conn=ls_conn,
            faiss_index=args.faiss_index,
            faiss_meta=args.faiss_meta,
            latdeu_db=args.latdeu_db,
            model=args.model,
            k=args.k
        )

        # Guard-rails morfológicos vs entrada atual, se possível
        if entry_id and cur_ret and res.get("suggestion"):
            cur_ret.execute("SELECT morfologia FROM entries WHERE id=?", (entry_id,))
            row = cur_ret.fetchone()
            entry_morph = row[0] if row else ""
            entry_class = morph_class_from_entry(entry_morph)
            ls_class    = res["suggestion"].get("ls_morph_class") or "unknown"
            conflict = morph_conflicts(ls_class, entry_class)
            res["guard_morph_conflict"] = bool(conflict)
            if conflict and res["suggestion"]["confianca"] == "high":
                res["suggestion"]["confianca"] = "med"
                res["suggestion"]["motivo"] += "; conflito morfológico (LS vs entrada)"

        # Vizinhança alpha opcional para auditoria humana
        if entry_id and cur_ret:
            res["alpha_neighbors"] = alpha_neighbors(cur_ret, entry_id, args.alpha_window)

        if entry_id:
            res["entry_id"] = entry_id

        print(json.dumps(res, ensure_ascii=False))

    ls_conn.close()
    if ret_conn: ret_conn.close()

if __name__ == "__main__":
    main()
