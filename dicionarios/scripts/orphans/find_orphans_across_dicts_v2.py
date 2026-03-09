#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
find_orphans_across_dicts_v2.py

Procura lemmas do retificado_v2.db que não aparecem em NENHUM dos dicionários
de referência (ls_dict.db novo, gaffiot.db novo, token_latim_german.sqlite).

Compatível com os esquemas:

- retificado_v2.db
  - VIEW entries(id, lema_canonico, morfologia, definicao, notas, conf, ...)
  - TABLE entry(id TEXT UNIQUE, lemma TEXT, lemma_sort TEXT, ...)

- ls_dict.db (NOVO formato “parecido com o Gaffiot”)
  - TABLE entry(entry_id, id TEXT UNIQUE, lemma, lemma_sort, ...)
  - TABLE entry_form(form_norm)

- gaffiot.db
  - TABLE entry(entry_id, lemma, lemma_sort, ...)
  - TABLE entry_form(form_norm)

- token_latim_german.sqlite (Lat→Deu)
  - TABLE FORM(form_norm, ...); INDEX recomendado em form_norm
  - TABLE VOC(vok_id, latin, html, ...); INDEX recomendado em vok_id

Saída (TSV) foca nos órfãos. Se quiser listar todos, use --emit-found.
"""

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
import unicodedata
from typing import Dict, List, Optional, Sequence, Tuple

# -------------------------
# Normalização de texto
# -------------------------

REMOVE_CHARS = r"[\^\·\.\(\)\[\]\{\}°˘̆̄¯´`ʹ,'’\"“”/\\;:!?*]"


# Tabela de confusáveis/ligaturas mais relevantes p/ Latim
CONFUSABLE_MAP = {
    # ligaturas
    "\u00e6": "ae",  # æ
    "\u0153": "oe",  # œ
    # variantes de y com diacríticos (muitos viram 'y' após strip de combining, mas pré-map ajuda)
    "\u0233": "y",  # ȳ
    "\u1e8f": "y",  # ẏ
    "\u1ef3": "y",  # ỳ
    "\u00fd": "y",  # ý
    "\u00ff": "y",  # ÿ
    "\u0177": "y",  # ŷ
    "\u1e99": "y",  # ẙ
    # homógrafos visuais comuns
    "\u0443": "y",  # у (cirílico)
    "\u03c5": "y",  # υ (grego minúsc.)
    "\u03a5": "y",  # Υ (grego maiúsc., avalie se quer)
    "\u02bc": "'",  # ʼ -> '
    "\u2019": "'",  # ’ -> '
    "\u2018": "'",  # ‘ -> '
    "\u2010": "-",  # ‐ -> -
    "\u2011": "-",  # - -> -
    "\u2013": "-",  # – -> -
    "\u2014": "-",  # — -> -
    "\u00ad": "",  # soft hyphen
    "\u017f": "s",  # ſ long s
    "\u0131": "i",  # ı dotless i
    "\u0130": "i",  # İ dotted I
}

TRANS = str.maketrans(CONFUSABLE_MAP)


def normalize_query(s: str, *, fold_jv=False) -> str:
    if not s:
        return s
    # 1) compatibilidade e largura
    s = unicodedata.normalize("NFKC", s)
    # 2) mapear confusáveis/ligaturas pontuais
    s = s.translate(TRANS)
    # 3) casefold (melhor p/ ß etc.)
    s = s.casefold()
    # 4) decompor e remover diacríticos
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("ſ", "s").replace("æ", "ae").replace("œ", "oe")
    # 5) colapsos opcionais
    if fold_jv:
        s = s.replace("j", "i").replace("v", "u")
    # 6) normalizar espaços/pontuação leve
    s = re.sub(r"\s+", " ", s).strip()
    return s


def strip_accents(s: str) -> str:
    if not s:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def norm_latin(
    s: str,
    normalize_jv: bool = False,
    collapse_hyphen: bool = True,
    keep_spaces: bool = False,
) -> str:
    """
    Normalização leve para *comparar* strings entre dicionários.
    Não altera o conteúdo do DB; serve só para matching.
    """
    if s is None:
        s = ""
    x = strip_accents(s).lower()
    x = x.translate(TRANS)
    if normalize_jv:
        x = x.replace("j", "i").replace("v", "u")
    x = x.replace("ſ", "s").replace("æ", "ae").replace("œ", "oe")
    x = re.sub(REMOVE_CHARS, "", x)
    if collapse_hyphen:
        x = re.sub(r"[-_]", "", x)
    if keep_spaces:
        x = re.sub(r"\s+", " ", x).strip()
    else:
        x = re.sub(r"\s+", "", x).strip()
    return x


# -------------------------
# Conexão SQLite
# -------------------------


def connect_sqlite(path: Optional[str]) -> Optional[sqlite3.Connection]:
    if not path:
        return None
    if not os.path.exists(path):
        print(f"[warn] DB não encontrado: {path}", file=sys.stderr)
        return None
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


# -------------------------
# Provas de existência por dicionário
# -------------------------


def probe_gaffiot(
    conn: Optional[sqlite3.Connection], lemma: str, lemma_norm: str
) -> List[str]:
    """
    gaffiot.db (novo):
      - entry(lemma)         -> prova: gaffiot:entry.lemma
      - entry(lemma_sort)    -> prova: gaffiot:entry.lemma_sort
      - entry_form(form_norm)-> prova: gaffiot:entry_form
    """
    hits: List[str] = []
    if conn is None:
        return hits
    cur = conn.cursor()
    # lemma
    try:
        cur.execute("SELECT 1 FROM entry WHERE lemma = ? LIMIT 1", (lemma,))
        if cur.fetchone():
            hits.append("gaffiot:entry.lemma")
    except sqlite3.OperationalError:
        pass
    # lemma_sort
    try:
        cur.execute("SELECT 1 FROM entry WHERE lemma_sort = ? LIMIT 1", (lemma_norm,))
        if cur.fetchone():
            hits.append("gaffiot:entry.lemma_sort")
    except sqlite3.OperationalError:
        pass
    # forms
    try:
        cur.execute(
            "SELECT 1 FROM entry_form WHERE form_norm = ? LIMIT 1", (lemma_norm,)
        )
        if cur.fetchone():
            hits.append("gaffiot:entry_form")
    except sqlite3.OperationalError:
        pass
    return hits


def probe_ls(
    conn: Optional[sqlite3.Connection], lemma: str, lemma_norm: str
) -> List[str]:
    """
    ls_dict.db (novo):
      - entry(lemma)         -> ls:entry.lemma
      - entry(lemma_sort)    -> ls:entry.lemma_sort
      - entry_form(form_norm)-> ls:entry_form
    Obs: há colunas adicionais (itype_json etc.), mas não são necessárias para a prova.
    """
    hits: List[str] = []
    if conn is None:
        return hits
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM entry WHERE lemma = ? LIMIT 1", (lemma,))
        if cur.fetchone():
            hits.append("ls:entry.lemma")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("SELECT 1 FROM entry WHERE lemma_sort = ? LIMIT 1", (lemma_norm,))
        if cur.fetchone():
            hits.append("ls:entry.lemma_sort")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute(
            "SELECT 1 FROM entry_form WHERE form_norm = ? LIMIT 1", (lemma_norm,)
        )
        if cur.fetchone():
            hits.append("ls:entry_form")
    except sqlite3.OperationalError:
        pass
    return hits


def probe_latdeu(
    conn: Optional[sqlite3.Connection], lemma_norm: str, lemma_like_probe: str = ""
) -> List[str]:
    """
    token_latim_german.sqlite:
      - FORM(form_norm) -> latdeu:FORM
      - VOC(latin) fallback “like” -> latdeu:VOC (depois reconfirma por normalização)

    Dica de performance:
      CREATE INDEX IF NOT EXISTS idx_form_norm_vok ON FORM(form_norm, vok_id);
      CREATE INDEX IF NOT EXISTS idx_form_norm ON FORM(form_norm);
      CREATE INDEX IF NOT EXISTS idx_voc_vok ON VOC(vok_id);
    """
    hits: List[str] = []
    if conn is None:
        return hits
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM FORM WHERE form_norm = ? LIMIT 1", (lemma_norm,))
        if cur.fetchone():
            hits.append("latdeu:FORM")
            return hits
    except sqlite3.OperationalError:
        pass

    # fallback no VOC.latin (LIKE) + checagem por normalização
    try:
        like_probe = lemma_like_probe or lemma_norm[:5]
        if like_probe:
            wildcard = f"%{like_probe}%"
            cur.execute(
                "SELECT latin FROM VOC WHERE latin LIKE ? LIMIT 200", (wildcard,)
            )
            for r in cur.fetchall() or []:
                if norm_latin(r["latin"]) == lemma_norm:
                    hits.append("latdeu:VOC")
                    break
    except sqlite3.OperationalError:
        pass

    return hits


def consulta_whitaker_words(palavras: list[str]):
    # path exec whitakers-words/bin/words
    # working dir whitakers-words
    import subprocess

    # Remover toda a acentuação das palavras
    palavras = [strip_accents(p) for p in palavras if p and p.lower().strip()]

    # Remover duplicatas
    palavras = set(palavras)

    # Se tiver caracteres especiais, remover
    palavras = [re.sub(r"[^a-zA-Z0-9]", "", p) for p in palavras if p]

    if len(palavras) == 0:
        return ""

    # Ordenar as palavras
    palavras = sorted(palavras)

    try:
        # print(f"Consultando whitakers-words para: {palavras}")
        newline_cmd_echo = "\\n" * len(palavras)
        result = subprocess.run(
            palavras,
            executable="bin/words",
            capture_output=True,
            cwd="whitakers-words",
            input=newline_cmd_echo,
            text=True,
            check=True,
        )

        result_stdout = result.stdout
        result_stdout = result_stdout.replace("MORE - hit RETURN/ENTER to continue", "")

        # print("Resultado da consulta:", result_stdout)

        # Remover linhas que tem UNKNOWN
        result_stdout = "\n".join(
            line for line in result_stdout.splitlines() if "UNKNOWN" not in line
        )

        # Se o resultado for vazio, retornar vazio
        if not result_stdout.strip():
            return ""

        return result_stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"[error] Erro ao consultar whitakers-words: {e}", file=sys.stderr)
        print(f"[error] Saída de erro: {e.stderr}", file=sys.stderr)
        print(f"[error] Saída padrão: {e.stdout}", file=sys.stderr)

    return ""


def probe_whitakers_words(lemma_norm: str, lemma_like_probe: str = "") -> List[str]:
    """
    whitakers-words:
      - consulta via subprocess para palavras normatizadas
    """
    hits: List[str] = []
    if not lemma_norm:
        return hits

    # Consulta a whitakers-words
    result = consulta_whitaker_words([lemma_norm, lemma_like_probe])
    if len(result.strip()) > 0:
        hits.append("whitaker:found")
    return hits


# -------------------------
# Orquestração
# -------------------------


def read_retificado_entries(
    conn_r: sqlite3.Connection, conf_filter: str = "any", limit: int = 0
) -> Sequence[sqlite3.Row]:
    """
    retificado_v2.db:
      Preferimos a VIEW 'entries' (id, lema_canonico, morfologia, definicao, conf, ...).
      Se não existir, caímos no SELECT mínimo via 'entry'.
    """
    cur = conn_r.cursor()
    # Tenta a VIEW 'entries'
    try:
        where, params = "", []
        if conf_filter != "any":
            where = "WHERE conf = ?"
            params.append(f"conf:{conf_filter}")
        q = f"""
            SELECT id, lema_canonico, morfologia, definicao, COALESCE(conf,'') AS conf
            FROM entries
            {where}
            ORDER BY lema_canonico
        """
        cur.execute(q, params)
        rows = cur.fetchall()
        if limit and limit > 0:
            rows = rows[:limit]
        return rows
    except sqlite3.OperationalError:
        pass

    # Fallback: 'entry' (mapeia manualmente)
    where, params = "", []
    if conf_filter != "any":
        where = "WHERE conf = ?"
        params.append(f"conf:{conf_filter}")
    q = f"""
        SELECT
          id,
          lemma AS lema_canonico,
          COALESCE(morph_render,'') AS morfologia,
          COALESCE(definicao,'')    AS definicao,
          COALESCE(conf,'')         AS conf
        FROM entry
        {where}
        ORDER BY lemma
    """
    cur.execute(q, params)
    rows = cur.fetchall()
    if limit and limit > 0:
        rows = rows[:limit]
    return rows


def main():
    ap = argparse.ArgumentParser(
        description="Lista lemmas do retificado_v2 que estão ausentes dos dicionários de referência (LS novo, Gaffiot novo, Lat→Deu)."
    )
    ap.add_argument("--retificado", required=True, help="Caminho para retificado_v2.db")
    ap.add_argument(
        "--ls", required=True, help="Caminho para ls_dict.db (novo formato)"
    )
    ap.add_argument(
        "--gaffiot", required=True, help="Caminho para gaffiot.db (novo formato)"
    )
    ap.add_argument(
        "--latdeu", required=True, help="Caminho para token_latim_german.sqlite"
    )
    ap.add_argument(
        "--conf-filter",
        choices=["low", "med", "high", "any"],
        default="any",
        help="Filtra por conf em retificado_v2 (ex.: conf:high). Padrão: any",
    )
    ap.add_argument(
        "--limit", type=int, default=0, help="Processa no máximo N itens (0 = todos)"
    )
    ap.add_argument(
        "--out",
        default="orphans_v2.tsv",
        help="TSV de saída (somente órfãos por padrão)",
    )
    ap.add_argument(
        "--emit-found",
        action="store_true",
        help="Se setado, também escreve linhas ENCONTRADAS",
    )
    ap.add_argument(
        "--no-jv",
        action="store_true",
        help="Não normalizar j→i / v→u (matching mais estrito)",
    )
    args = ap.parse_args()

    # Conexões
    conn_r = connect_sqlite(args.retificado)
    conn_ls = connect_sqlite(args.ls)
    conn_gf = connect_sqlite(args.gaffiot)
    conn_ld = connect_sqlite(args.latdeu)

    if not conn_r:
        print("[fatal] retificado_v2.db não encontrado.", file=sys.stderr)
        sys.exit(2)

    # Lê entradas do retificado (VIEW entries preferida)
    entries = read_retificado_entries(
        conn_r, conf_filter=args.conf_filter, limit=args.limit
    )

    # Garante diretório de saída
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    total = 0
    orphans = 0
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(
            [
                "entry_id",
                "lemma",
                "lemma_norm",
                "conf_retificado",
                "morfologia",
                "found_count",
                "found_where_csv",
                "consensus_note",
            ]
        )

        for r in entries:
            total += 1
            entry_id = r["id"]
            lemma = r["lema_canonico"] or ""
            morph = r["morfologia"] or ""
            conf = r["conf"] or ""

            # Normalização: por padrão normalizamos j/v (p/ conciliar bases antigas x modernas).
            ln = norm_latin(lemma, normalize_jv=(not args.no_jv))

            if not ln:
                if args.emit_found:
                    w.writerow(
                        [
                            entry_id,
                            lemma,
                            ln,
                            conf,
                            morph,
                            0,
                            "",
                            "skip:lemma_norm_vazia",
                        ]
                    )
                continue

            # Sondagens nos 3 dicionários
            found_where: List[str] = []
            found_where += probe_ls(conn_ls, lemma, ln)
            found_where += probe_gaffiot(conn_gf, lemma, ln)
            found_where += probe_latdeu(conn_ld, ln)
            found_where += probe_whitakers_words(ln, lemma_like_probe=lemma)

            found_count = len(set(found_where))

            if found_count == 0:
                orphans += 1
                w.writerow(
                    [
                        entry_id,
                        lemma,
                        ln,
                        conf,
                        morph,
                        0,
                        "",
                        "NOT FOUND in ls+gaffiot+latdeu",
                    ]
                )
            else:
                if args.emit_found:
                    # Se ao menos 1 confirma, já dá para “subir” um nível de confiança em pipelines posteriores.
                    # Aqui apenas sugerimos.
                    suggest = "≥1 source confirms (ok to consider raising confidence)"
                    if found_count >= 2:
                        suggest = "≥2 sources confirm (strong consensus)"
                    w.writerow(
                        [
                            entry_id,
                            lemma,
                            ln,
                            conf,
                            morph,
                            found_count,
                            ",".join(sorted(set(found_where))),
                            suggest,
                        ]
                    )

    print(f"[ok] processados={total} orfaos={orphans} -> {args.out}")


if __name__ == "__main__":
    main()
