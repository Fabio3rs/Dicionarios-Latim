#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
faiss_ollama_multi.py — Indexador/Pesquisador FAISS unificado (novo formato)

Suporta diretamente os esquemas "novos" que você compartilhou:
- ls_dict.db (estilo LS modernizado)
- gaffiot.db  (1934 unicode modernizado)
- retificado_v2.db (seu passo intermediário)

Objetivo:
- Construir um único índice FAISS combinando 1..N dicionários.
- Consultar o índice por texto (lemma, forma, ou gloss) e retornar os melhores
  matches com metadados ricos (incluindo fonte e campos úteis).

Embeddings:
- Usa servidor Ollama (HTTP) com um modelo de embeddings (ex.: nomic-embed-text).
  Você pode trocar por outro modelo suportado pelo Ollama que produza embeddings.

Arquivos gerados:
- <outdir>/index.faiss        -> índice FAISS em disco
- <outdir>/meta.jsonl         -> metadados (JSON Lines), 1 por vetor, na mesma ordem

Uso:
  Construir a partir de múltiplos bancos:
    python3 faiss_ollama_multi.py build \
      --outdir index_mix \
      --model nomic-embed-text \
      --ls ls_dict.db \
      --gaffiot gaffiot.db \
      --retificado retificado_v2.db

  Consultar:
    python3 faiss_ollama_multi.py query \
      --index index_mix/index.faiss \
      --meta  index_mix/meta.jsonl \
      --model nomic-embed-text \
      --k 10 \
      --text "abavus trisavô"

Requisitos:
  pip install faiss-cpu requests beautifulsoup4
  (e deixe o servidor Ollama rodando com o modelo escolhido já baixado)

Observação importante:
- Este script não realiza normalização u/v nem i/j por padrão. Se desejar,
  você pode ativar a normalização via flag --norm-uvij no build e/ou query.
"""

import argparse
import dataclasses
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
from typing import Dict, Iterable, List, Optional, Tuple

import faiss
import requests
from bs4 import BeautifulSoup

# ---------------------------- Utilidades ------------------------------------


def u_lower(s: str) -> str:
    return s.lower()


def strip_ws(s: Optional[str]) -> str:
    return s.strip() if s else ""


def norm_nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s) if s is not None else ""


_UV_RE = re.compile(r"[uv]", re.IGNORECASE)
_IJ_RE = re.compile(r"[ij]", re.IGNORECASE)


def normalize_uvij(s: str) -> str:
    """Normaliza u/v -> v, i/j -> i (para busca opcional)."""
    if s is None:
        return ""
    # Substituímos tanto maiús. quanto minús. por minúsculas normalizadas
    s2 = unicodedata.normalize("NFKD", s).casefold()
    s2 = _UV_RE.sub("v", s2)
    s2 = _IJ_RE.sub("i", s2)
    return unicodedata.normalize("NFKC", s2)


def html_to_text(html: Optional[str]) -> str:
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        # Captura info útil como <span class="g">Verb, a-Konj.</span>
        return soup.get_text(" ", strip=True)
    except Exception:
        return html


def coalesce(*vals) -> str:
    for v in vals:
        if v and str(v).strip():
            return str(v)
    return ""


# ----------------------- Cliente Embeddings (Ollama) -------------------------


def embed_ollama(
    texts: List[str],
    model: str = "nomic-embed-text",
    url: str = "http://localhost:11434/api/embeddings",
    timeout: float = 60.0,
) -> List[List[float]]:
    """Obtém embeddings do Ollama. Retorna lista de vetores."""
    out = []
    sess = requests.Session()
    for t in texts:
        payload = {"model": model, "prompt": t}
        r = sess.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        vec = data.get("embedding") or data.get("embeddings")
        if vec is None:
            raise RuntimeError(
                f"Sem campo 'embedding(s)' na resposta Ollama: {data.keys()}"
            )
        out.append(vec)
    return out


# --------------------------- Metadados canônicos -----------------------------


@dataclasses.dataclass
class EntryMeta:
    # campos mínimos para navegação
    id: str  # id único global no índice (ex.: "ls:12345")
    source: str  # "ls", "gaffiot", "retificado"
    entry_id: int  # id interno da fonte
    lemma: str
    lemma_sort: str
    forms: List[str]
    gloss: str  # campo principal para rank/recall
    pos: str = ""
    gender: str = ""
    indeclinable: int = 0
    itype: str = ""  # LS/gaffiot
    head_raw: str = ""  # LS/gaffiot
    morph: str = ""  # retificado_v2 (morph_render)
    notes: str = ""  # notas/gloss_raw
    extra: Dict = dataclasses.field(default_factory=dict)

    def to_text_for_embedding(self, norm_uvij: bool = False) -> str:
        fields = [
            self.lemma,
            " ".join(self.forms or []),
            self.gloss,
            self.notes,
            self.pos,
            self.gender,
            self.itype,
            self.morph,
            self.head_raw,
        ]
        txt = " | ".join([f for f in fields if f])
        return normalize_uvij(txt) if norm_uvij else txt

    def to_json(self) -> Dict:
        d = dataclasses.asdict(self)
        # forms como lista; manter
        return d


# ---------------------- Extratores por Banco de Dados ------------------------


def extract_ls_entries(db_path: str) -> Iterable[EntryMeta]:
    """
    ls_dict.db (novo): tabelas: entry, entry_form, sense, citation...
    Regra: priorizar PT (tr_gloss_pt, tr_trad_pt, tr_notas) e usar EN
    (sense.gloss_en / gloss_raw) como fallback.
    """
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # forms por entry_id
    forms_by_entry = {}
    for r in con.execute("SELECT entry_id, form FROM entry_form"):
        forms_by_entry.setdefault(r["entry_id"], []).append(r["form"])

    # gloss EN (fallback) por entry_id, agregado dos sentidos
    gloss_by_entry_en = {}
    for r in con.execute(
        """
        SELECT entry_id, COALESCE(gloss_en, gloss_raw) AS g
        FROM sense
    """
    ):
        g = (r["g"] or "").strip()
        if g:
            gloss_by_entry_en.setdefault(r["entry_id"], []).append(g)

    cur = con.cursor()
    cur.execute(
        """
        SELECT e.entry_id, e.lemma, e.lemma_sort, e.pos, e.gen_text, e.indeclinable,
               e.itype, e.head_raw,
               COALESCE(e.tr_gloss_pt, '')   AS tr_gloss_pt,
               COALESCE(e.tr_trad_pt, '')    AS tr_trad_pt,
               COALESCE(e.tr_notas,   '')    AS tr_notas
        FROM entry e
    """
    )

    for r in cur.fetchall():
        eid = int(r["entry_id"])

        # 1) PT primeiro
        pt_chunks = [
            r["tr_gloss_pt"].strip(),
            r["tr_trad_pt"].strip(),
            r["tr_notas"].strip(),
        ]
        pt_chunks = [c for c in pt_chunks if c]
        gloss_pt = " ".join(pt_chunks)

        # 2) Fallback EN (agregado dos senses)
        gloss_en = " ".join(gloss_by_entry_en.get(eid, []))

        # 3) Escolha final (PT > EN)
        gloss_final = gloss_pt if gloss_pt else gloss_en

        yield EntryMeta(
            id=f"ls:{eid}",
            source="ls",
            entry_id=eid,
            lemma=r["lemma"],
            lemma_sort=r["lemma_sort"],
            forms=forms_by_entry.get(eid, []),
            gloss=gloss_final,
            pos=coalesce(r["pos"]),
            gender=coalesce(r["gen_text"]),
            indeclinable=int(r["indeclinable"] or 0),
            itype=coalesce(r["itype"]),
            head_raw=coalesce(r["head_raw"]),
            morph="",
            notes="",  # notas em PT já entram no gloss_final (tr_notas)
            extra={"lang_priority": "pt_first", "has_pt": bool(gloss_pt)},
        )
    con.close()


def extract_gaffiot_entries(db_path: str) -> Iterable[EntryMeta]:
    """
    gaffiot.db (novo): tabelas: entry, entry_form, sense
    entry(entry_id, lemma, lemma_sort, pos, gender, indeclinable, itype, gen_text, pron, etym, head_raw, xml_fragment)
    sense(level, gloss_fr, gloss_raw)
    """
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    forms_by_entry = {}
    for r in con.execute("SELECT entry_id, form FROM entry_form"):
        forms_by_entry.setdefault(r["entry_id"], []).append(r["form"])

    gloss_by_entry = {}
    for r in con.execute(
        "SELECT entry_id, COALESCE(gloss_fr, gloss_raw) AS g FROM sense"
    ):
        if not r["g"]:
            continue
        gloss_by_entry.setdefault(r["entry_id"], []).append(r["g"])

    cur = con.cursor()
    cur.execute(
        """
        SELECT entry_id, lemma, lemma_sort, pos, gender, indeclinable,
               itype, gen_text, head_raw
        FROM entry
    """
    )
    for r in cur.fetchall():
        eid = int(r["entry_id"])
        gloss = " ".join(gloss_by_entry.get(eid, []))
        m = EntryMeta(
            id=f"gaffiot:{eid}",
            source="gaffiot",
            entry_id=eid,
            lemma=r["lemma"],
            lemma_sort=r["lemma_sort"],
            forms=forms_by_entry.get(eid, []),
            gloss=gloss,
            pos=coalesce(r["pos"]),
            gender=coalesce(r["gender"] or r["gen_text"]),
            indeclinable=int(r["indeclinable"] or 0),
            itype=coalesce(r["itype"]),
            head_raw=coalesce(r["head_raw"]),
            morph="",
            notes="",
            extra={},
        )
        yield m
    con.close()


def extract_retificado_entries(db_path: str) -> Iterable[EntryMeta]:
    """
    retificado_v2.db: tabelas: entry, entry_form, sense
    entry(morph_render, definicao, notas, pos, gender, ...)
    """
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    forms_by_entry = {}
    for r in con.execute("SELECT entry_id, form FROM entry_form"):
        forms_by_entry.setdefault(r["entry_id"], []).append(r["form"])

    cur = con.cursor()
    cur.execute(
        """
        SELECT entry_id, lemma, lemma_sort, pos, gender, indeclinable,
               morph_render, definicao, notas, head_raw
        FROM entry
    """
    )
    for r in cur.fetchall():
        eid = int(r["entry_id"])
        gloss_parts = [strip_ws(r["definicao"]), strip_ws(r["notas"])]
        gloss = " ".join([p for p in gloss_parts if p])
        m = EntryMeta(
            id=f"retificado:{eid}",
            source="retificado",
            entry_id=eid,
            lemma=r["lemma"],
            lemma_sort=r["lemma_sort"],
            forms=forms_by_entry.get(eid, []),
            gloss=gloss,
            pos=coalesce(r["pos"]),
            gender=coalesce(r["gender"]),
            indeclinable=int(r["indeclinable"] or 0),
            itype="",
            head_raw=coalesce(r["head_raw"]),
            morph=coalesce(r["morph_render"]),
            notes=coalesce(r["notas"]),
            extra={},
        )
        yield m
    con.close()


def _safe_decode(val):
    if val is None:
        return None
    if isinstance(val, bytes):
        for enc in ("utf-8", "cp1252", "latin-1"):
            try:
                return val.decode(enc)
            except UnicodeDecodeError:
                pass
        # último recurso: não falha
        return val.decode("utf-8", errors="replace")
    return val  # já é str

def extract_latin_german_entries(db_path: str) -> Iterable[EntryMeta]:
    """
    Extrai entradas do token_latim_german.sqlite.
    Usa VOC.latin como lema e VOC.desc (ou html limpo) como gloss.
    Junta formas de FORM e GRAMMAR.
    """
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.text_factory = _safe_decode  # para lidar com bytes

    # formas da FORM
    forms_by_vok = {}
    for r in con.execute("SELECT vok_id, form FROM FORM"):
        if r["form"]:
            forms_by_vok.setdefault(r["vok_id"], []).append(r["form"])

    # formas da GRAMMAR
    for r in con.execute("SELECT vok_id, form FROM GRAMMAR"):
        if r["form"]:
            forms_by_vok.setdefault(r["vok_id"], []).append(r["form"])

    cur = con.cursor()
    cur.execute(
        """
        SELECT vok_id, latin, desc, html, key, grammar, typnr
        FROM VOC
    """
    )
    for r in cur.fetchall():
        vok = r["vok_id"]
        lemma = r["latin"] or ""
        gloss = r["desc"] or ""

        # fallback: se desc vazio, tenta extrair do html
        if not gloss and r["html"]:
            soup = BeautifulSoup(r["html"], "html.parser")
            gloss = soup.get_text(" ", strip=True)

        yield EntryMeta(
            id=f"lg:{vok}",
            source="latin_german",
            entry_id=vok,
            lemma=lemma,
            lemma_sort=lemma.lower(),
            forms=forms_by_vok.get(vok, []),
            gloss=gloss,
            pos="",  # pode tentar extrair de grammar/html
            gender="",
            indeclinable=0,
            itype="",
            head_raw="",
            morph=r["grammar"] or "",
            notes="",
            extra={"typnr": r["typnr"]},
        )
    con.close()


# -------------------------- Build & Query pipeline ---------------------------


def build_index(
    outdir: str,
    model: str,
    ls_path: Optional[str] = None,
    gaffiot_path: Optional[str] = None,
    retificado_path: Optional[str] = None,
    batch_size: int = 64,
    norm_uvij: bool = False,
) -> None:
    os.makedirs(outdir, exist_ok=True)
    meta_path = os.path.join(outdir, "meta.jsonl")
    index_path = os.path.join(outdir, "index.faiss")

    metas: List[EntryMeta] = []
    if ls_path:
        print(f"[build] Extraindo LS de {ls_path} ...", file=sys.stderr)
        metas.extend(list(extract_ls_entries(ls_path)))
    if gaffiot_path:
        print(f"[build] Extraindo Gaffiot de {gaffiot_path} ...", file=sys.stderr)
        metas.extend(list(extract_gaffiot_entries(gaffiot_path)))
    if retificado_path:
        print(f"[build] Extraindo Retificado de {retificado_path} ...", file=sys.stderr)
        metas.extend(list(extract_retificado_entries(retificado_path)))

    print(f"[build] Extraindo Latin-German de token_latim_german.sqlite ...", file=sys.stderr)
    metas.extend(extract_latin_german_entries("token_latim_german.sqlite"))


    if not metas:
        raise SystemExit("Nenhuma fonte fornecida (use --ls/--gaffiot/--retificado).")

    print(f"[build] Total de entradas: {len(metas)}", file=sys.stderr)

    # Texto para embedding
    texts = [m.to_text_for_embedding(norm_uvij=norm_uvij) for m in metas]

    # Embedding em lotes
    vecs: List[List[float]] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        embs = embed_ollama(batch, model=model)
        vecs.extend(embs)
        print(f"[build] Embeddings {i+len(batch)}/{len(texts)}", file=sys.stderr)

    # FAISS -> usamos IndexFlatIP (cosine via normalização)
    import numpy as np

    X = np.array(vecs, dtype="float32")

    # Normaliza vetores (cosine similarity)
    faiss.normalize_L2(X)

    index = faiss.IndexFlatIP(X.shape[1])
    index.add(X)
    faiss.write_index(index, index_path)
    print(f"[build] Índice salvo em {index_path}", file=sys.stderr)

    # Escreve metadados
    with open(meta_path, "w", encoding="utf-8") as f:
        for m in metas:
            f.write(json.dumps(m.to_json(), ensure_ascii=False) + "\n")
    print(f"[build] Metadados salvos em {meta_path}", file=sys.stderr)


def query_index(
    index_path: str,
    meta_path: str,
    model: str,
    text: str,
    k: int = 10,
    norm_uvij: bool = False,
) -> List[Dict]:
    # Carrega índice
    index = faiss.read_index(index_path)
    # Carrega metadados
    metas: List[Dict] = []
    with open(meta_path, "r", encoding="utf-8") as f:
        for line in f:
            metas.append(json.loads(line))

    # Embedda a consulta
    q_txt = normalize_uvij(text) if norm_uvij else text
    q_vec = embed_ollama([q_txt], model=model)[0]

    import numpy as np

    q = np.array([q_vec], dtype="float32")
    faiss.normalize_L2(q)

    D, I = index.search(q, min(k, len(metas)))
    out = []
    for score, idx in zip(D[0].tolist(), I[0].tolist()):
        if idx < 0 or idx >= len(metas):
            continue
        m = metas[idx].copy()
        m["_score"] = float(score)
        out.append(m)
    return out


# ------------------------------ CLI -----------------------------------------


def main():
    ap = argparse.ArgumentParser(
        description="FAISS + Ollama (LS/Gaffiot/Retificado v2)"
    )
    sub = ap.add_subparsers(dest="cmd")

    ap_build = sub.add_parser("build", help="Construir índice")
    ap_build.add_argument("--outdir", required=True)
    ap_build.add_argument("--model", default="nomic-embed-text")
    ap_build.add_argument("--ls", dest="ls_path", help="caminho para ls_dict.db")
    ap_build.add_argument(
        "--gaffiot", dest="gaffiot_path", help="caminho para gaffiot.db"
    )
    ap_build.add_argument(
        "--retificado", dest="retificado_path", help="caminho para retificado_v2.db"
    )
    ap_build.add_argument("--batch-size", type=int, default=64)
    ap_build.add_argument(
        "--norm-uvij", action="store_true", help="normalizar u/v e i/j antes de embedar"
    )

    ap_query = sub.add_parser("query", help="Consultar índice")
    ap_query.add_argument("--index", required=True, help="caminho index.faiss")
    ap_query.add_argument("--meta", required=True, help="caminho meta.jsonl")
    ap_query.add_argument("--model", default="nomic-embed-text")
    ap_query.add_argument("--k", type=int, default=10)
    ap_query.add_argument("--text", required=True)
    ap_query.add_argument(
        "--norm-uvij", action="store_true", help="normalizar u/v e i/j na consulta"
    )

    args = ap.parse_args()
    if args.cmd == "build":
        build_index(
            outdir=args.outdir,
            model=args.model,
            ls_path=args.ls_path,
            gaffiot_path=args.gaffiot_path,
            retificado_path=args.retificado_path,
            batch_size=args.batch_size,
            norm_uvij=args.norm_uvij,
        )
    elif args.cmd == "query":
        res = query_index(
            index_path=args.index,
            meta_path=args.meta,
            model=args.model,
            text=args.text,
            k=args.k,
            norm_uvij=args.norm_uvij,
        )
        for r in res:
            print(json.dumps(r, ensure_ascii=False))
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
