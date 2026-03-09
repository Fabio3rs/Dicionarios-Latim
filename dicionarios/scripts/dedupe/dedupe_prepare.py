#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dedupe_prepare.py — clustering por lema normalizado + evidência cruzada (LS, Lat→Deu, Gaffiot, Whitaker),
gates determinísticos e payload JSONL “N in → N out” para decisão de duplicatas via LLM.

Uso típico:
  python3 dedupe_prepare.py \
    --retificado-db retificado.db \
    --ls-db ls_dict.sqlite \
    --latdeu-db token_latim_german.sqlite \
    --gaffiot-db gaffiot.db \
    --whitaker-db whitaker.sqlite \
    --faiss-index index_ls/index.faiss \
    --faiss-meta  index_ls/meta.jsonl \
    --out dedupe_batches.jsonl \
    --limit-clusters 0

Observações:
- Só clusters com COUNT(*)>1 em entries.lema_canonico_norm entram.
- “Gates” automáticos:
    * Se todos os itens do cluster têm (lema_canonico, morfologia, definicao) idênticos → marcar “AUTO_KEEP_ALL” (sem LLM).
    * Se as morfologias caem em buckets distintos (e.g. ADJ vs NOUN) e há respaldo em Lat→Deu/LS → “AUTO_HOMONYMS_KEEP”.
    * Se houver hard-conflicts (mudança grande de lema sugerida em pipelines anteriores) → envia ao LLM com “risk=high”.
- O payload do LLM exige saída **1:1** com os IDs de entrada, escolhendo “keep:true/false” e, opcionalmente,
  “canonical_group” para homônimos (I/II) e “redirect_to” para descartes suaves.
"""

import argparse, json, sqlite3, re, os
from typing import List, Dict, Any, Tuple, Optional
from collections import defaultdict

# ------------------------- utils -------------------------

def open_db(path: Optional[str]) -> Optional[sqlite3.Connection]:
    if not path: return None
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn

def q(conn: sqlite3.Connection, sql: str, params: Tuple = ()) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()

def norm(s: str) -> str:
    s = s or ""
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("j","i").replace("v","u")
    return s

def bucket_from_morph(morf: str) -> str:
    m = (morf or "").lower()
    if not m: return "UNK"
    if m.startswith("v.") or "verb" in m: return "VERB"
    if "adj" in m or "adjet" in m: return "ADJ"
    if "s. masc" in m or "s. fem" in m or "s. neut" in m or "s." in m: return "NOUN"
    if "adv" in m: return "ADV"
    if "conj" in m: return "CONJ"
    if "prep" in m: return "PREP"
    return "UNK"

def faiss_topk(idx_path: str, meta_path: str, text: str, k: int = 3,
               ollama_model: str = "nomic-embed-text",
               ollama_url: str = "http://localhost:11434") -> List[Dict[str, Any]]:
    if not (idx_path and meta_path and text and text.strip()):
        return []
    import faiss, numpy as np, json as _json, requests
    metas = []
    with open(meta_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                metas.append(_json.loads(line))
    # embed
    payload = {"model": ollama_model, "input": [text]}
    r = requests.post(f"{ollama_url}/api/embed", json=payload, timeout=120)
    r.raise_for_status()
    emb = r.json().get("embeddings", [[]])[0]
    index = faiss.read_index(idx_path)
    D, I = index.search(np.array([emb], dtype="float32"), k)
    out = []
    for score, pos in zip(D[0].tolist(), I[0].tolist()):
        if 0 <= pos < len(metas):
            m = dict(metas[pos]); m["_score"] = float(score); m["_pos"] = int(pos)
            out.append(m)
    return out

# ------------------------- evidence fetchers -------------------------

def evidence_ls(ls: sqlite3.Connection, lemma_norm: str) -> List[Dict[str, Any]]:
    if not ls: return []
    rows = q(ls, """
      SELECT id, lemma, lemma_norm, pos, definition, tr_gloss_pt, tr_trad_pt
      FROM ls_entries
      WHERE lemma_norm = ? OR lemma = ?
      LIMIT 50
    """, (lemma_norm, lemma_norm))
    return [dict(r) for r in rows]

def evidence_latdeu(latdeu: sqlite3.Connection, forms: List[str]) -> List[Dict[str, Any]]:
    if not latdeu or not forms: return []
    # normalizar para a função/form_norm do seu DB
    forms = sorted(set(norm(x) for x in forms if x))
    if not forms: return []
    placeholders = ",".join("?" for _ in forms)
    rows = q(latdeu, f"""
      SELECT F.form, F.form_norm, F.bestimmung,
             V.id AS voc_id, V.vok_id, V.latin, V."desc" AS de_desc,
             V.grammar, V."key", V.typnr
      FROM FORM AS F
      JOIN VOC  AS V ON V.vok_id = F.vok_id
      WHERE F.form_norm IN ({placeholders})
      ORDER BY V.id LIMIT 200
    """, tuple(forms))
    return [dict(r) for r in rows]

def evidence_gaffiot(gaff: sqlite3.Connection, lemma_norm: str) -> List[Dict[str, Any]]:
    if not gaff: return []
    # ajuste ao seu schema do gaffiot.db
    # supondo tabela 'gaffiot' com colunas (lemma, lemma_norm, pos, definition, citations)
    rows = q(gaff, """
      SELECT lemma, lemma_norm, pos, definition, citations
      FROM gaffiot
      WHERE lemma_norm = ? OR lemma = ?
      LIMIT 50
    """, (lemma_norm, lemma_norm))
    return [dict(r) for r in rows]

def evidence_whitaker(wh: sqlite3.Connection, lemma_norm: str) -> List[Dict[str, Any]]:
    if not wh: return []
    # schema do whitaker.sqlite pode variar; ajuste se necessário
    rows = q(wh, """
      SELECT head AS lemma, LOWER(REPLACE(REPLACE(head,'J','I'),'V','U')) AS lemma_norm,
             pos, gloss AS definition
      FROM whitaker
      WHERE LOWER(REPLACE(REPLACE(head,'J','I'),'V','U')) = ?
      LIMIT 50
    """, (lemma_norm,))
    return [dict(r) for r in rows]

# ------------------------- neighbors -------------------------

def neighbors_by_id(conn: sqlite3.Connection, entry_id: str, n: int = 2) -> Dict[str, List[Tuple[str,str]]]:
    # extrai prefixo “chunk_x:y:” e ordinal final para ordenar
    m = re.match(r"^(chunk_\d+):(\d+):(\d+)$", entry_id)
    if not m:
        return {"prev": [], "next": []}
    g1, g2, g3 = m.group(1), int(m.group(2)), int(m.group(3))
    # lista vizinhos com mesma 'chunk_N' e ordinais próximos
    rows_prev = q(conn, """
      SELECT id, lema_canonico FROM entries
      WHERE id LIKE ? AND CAST(SUBSTR(id, INSTR(id,':')+1, INSTR(SUBSTR(id, INSTR(id,':')+1),':')-1) AS INT) <= ?
      ORDER BY id DESC LIMIT ?
    """, (f"{g1}:%", g2-1, n))
    rows_next = q(conn, """
      SELECT id, lema_canonico FROM entries
      WHERE id LIKE ? AND CAST(SUBSTR(id, INSTR(id,':')+1, INSTR(SUBSTR(id, INSTR(id,':')+1),':')-1) AS INT) >= ?
      ORDER BY id ASC LIMIT ?
    """, (f"{g1}:%", g2+1, n))
    return {
        "prev": [(r["id"], r["lema_canonico"]) for r in rows_prev],
        "next": [(r["id"], r["lema_canonico"]) for r in rows_next],
    }

# ------------------------- main prepare -------------------------

SYS_PROMPT = """Você é um(a) lexicógrafo(a) latino(a) estrito(a).
Tarefa: para CADA item do cluster, decidir se mantém ("keep": true) ou descarta ("keep": false).
- O número de itens de saída DEVE ser igual ao número de itens de entrada (N entra → N sai).
- Não RENOMEIE lemas automaticamente. Se identificar homônimos legítimos, use "canonical_group": "I" | "II" | "III"...
- Ao descartar, prefira “redirect” (mesmo lema apontando para o canon escolhido). Preencha "redirect_to": <id escolhido>.
- SÓ marque keep se houver respaldo em pelo menos 1 dicionário (LS, Lat-Deu, Gaffiot, Whitaker) OU se a morfologia fizer sentido no contexto.
- NUNCA altere o corpo do lema sem consenso de dicionários. Se necessário, sinalize "needs_manual": true.
Responda apenas JSON por linha, no mesmo número de linhas que a entrada."""

def build_user_payload(cluster_items: List[Dict[str, Any]],
                       evid: Dict[str, Any],
                       faiss_hits: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "cluster": cluster_items,
        "evidence": evid,
        "faiss": faiss_hits
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--retificado-db", required=True)
    ap.add_argument("--ls-db", required=True)
    ap.add_argument("--latdeu-db")
    ap.add_argument("--gaffiot-db")
    ap.add_argument("--whitaker-db")
    ap.add_argument("--faiss-index")
    ap.add_argument("--faiss-meta")
    ap.add_argument("--neighbors", type=int, default=2)
    ap.add_argument("--out", default="dedupe_batches.jsonl")
    ap.add_argument("--limit-clusters", type=int, default=0, help="0 = sem limite")
    args = ap.parse_args()

    conn = open_db(args.retificado_db)
    ls   = open_db(args.ls_db)
    ld   = open_db(args.latdeu_db) if args.latdeu_db else None
    gf   = open_db(args.gaffiot_db) if args.gaffiot_db else None
    wh   = open_db(args.whitaker_db) if args.whitaker_db else None

    # clusters com duplicata por lema_canonico_norm
    dup_rows = q(conn, """
      WITH d AS (
        SELECT lema_canonico_norm, COUNT(*) c
        FROM entries
        GROUP BY lema_canonico_norm
        HAVING c > 1
      )
      SELECT e.*
      FROM entries e
      JOIN d USING (lema_canonico_norm)
      ORDER BY e.lema_canonico_norm, e.lema_canonico, e.id;
    """)

    # materializa clusters
    by_key: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in dup_rows:
        by_key[r["lema_canonico_norm"]].append(dict(r))

    keys = list(by_key.keys())
    if args.limit_clusters > 0:
        keys = keys[:args.limit_clusters]

    out = open(args.out, "w", encoding="utf-8")
    total_sent = 0
    auto_kept = 0

    for k in keys:
        cluster = by_key[k]
        # pequenas heurísticas/gates
        all_same = len({(c["lema_canonico"], c["morfologia"], c["definicao"]) for c in cluster}) == 1
        if all_same:
            # gera saída "auto" sem LLM
            for c in cluster:
                record = {
                    "entry_id": c["id"],
                    "lemma": c["lema_canonico"],
                    "decision": {"keep": True, "reason": "AUTO_KEEP_ALL"},
                }
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                auto_kept += 1
            continue

        # evidencia cruzada
        lemmas_norm = sorted({norm(c["lema_canonico"]) for c in cluster})
        e_ls  = sum([evidence_ls(ls, ln) for ln in lemmas_norm], [])
        e_ld  = evidence_latdeu(ld, lemmas_norm) if ld else []
        e_gf  = sum([evidence_gaffiot(gf, ln) for ln in lemmas_norm], []) if gf else []
        e_wh  = sum([evidence_whitaker(wh, ln) for ln in lemmas_norm], []) if wh else []

        # buckets rápidos por morfologia local + latdeu
        buckets = {c["id"]: bucket_from_morph(c["morfologia"]) for c in cluster}
        if e_ld:
            # se todos itens caem em buckets bem distintos e há suporte → homônimos claros
            bset = set(buckets.values()) - {"UNK"}
            if len(bset) > 1:
                for idx, c in enumerate(cluster, start=1):
                    record = {
                        "entry_id": c["id"],
                        "lemma": c["lema_canonico"],
                        "decision": {
                            "keep": True,
                            "canonical_group": ["I","II","III","IV","V"][min(idx-1,4)],
                            "reason": "AUTO_HOMONYMS_KEEP (morph buckets distintos com suporte)"
                        },
                    }
                    out.write(json.dumps(record, ensure_ascii=False) + "\n")
                continue

        # FAISS (contexto: definicoes)
        faiss_hits = {}
        if args.faiss_index and args.faiss_meta:
            for c in cluster:
                ctx = (c["definicao"] or "")[:600]
                if ctx:
                    faiss_hits[c["id"]] = faiss_topk(args.faiss_index, args.faiss_meta, ctx, k=3)
        # vizinhança
        neigh = {c["id"]: neighbors_by_id(conn, c["id"], n=args.neighbors) for c in cluster}

        # construir prompt p/ Responses/Batch
        user_payload = build_user_payload(
            cluster_items=[{
                "id": c["id"],
                "lemma": c["lema_canonico"],
                "morph": c["morfologia"],
                "def": c["definicao"],
                "conf": c["conf"],
                "neighbors": neigh.get(c["id"], {})
            } for c in cluster],
            evid={
                "LS": e_ls, "LATDEU": e_ld, "GAFFIOT": e_gf, "WHITAKER": e_wh,
                "morph_buckets": buckets
            },
            faiss=faiss_hits
        )

        # linha no formato JSONL (uma “tarefa” por cluster)
        # você pode alimentar isso no seu call_open_ai.py (Batches)
        line = {
            "custom_id": f"dedupe::{k}",
            "method": "POST",
            "url": "/v1/responses",
            "body": {
                "presence_penalty": 0.0,
                "frequency_penalty": 0.0,
                "reasoning_effort": "low",
                "input": [
                    {"role":"system","content": SYS_PROMPT},
                    {"role":"user","content": json.dumps(user_payload, ensure_ascii=False)}
                ],
                "max_output_tokens": 2000,
                "response_format": {"type":"jsonl"}
            }
        }
        out.write(json.dumps(line, ensure_ascii=False) + "\n")
        total_sent += 1

    out.close()
    print(json.dumps({
        "clusters": len(keys),
        "auto_kept": auto_kept,
        "sent_to_llm": total_sent,
        "outfile": os.path.abspath(args.out)
    }, ensure_ascii=False))

if __name__ == "__main__":
    main()
