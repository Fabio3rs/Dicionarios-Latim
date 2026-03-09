#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Atualiza retificado_v2.db com classes morfológicas extraídas do Lat→Deu,
lendo a faixa <span class="g">…</span> do campo VOC.html via BeautifulSoup.

- Mapeia:
  Verb, a-Konj.      -> verb_class='a'     (-āre)
  Verb, e-Konj.      -> verb_class='e'     (-ēre)
  Verb, i-Konj.      -> verb_class='i'     (-īre)
  Verb, kons. Konj.  -> verb_class='cons'  (-ere, 3ª)

  Adj., a/o-Dekl.        -> adj_class='a/o'
  Adj., Mischkl., 2-endig -> adj_class='3rd-2end'
  Adj., Mischkl., 3-endig -> adj_class='3rd-3end' (se aparecer)

  Subst., a-Dekl.    -> decl_class='1st'
  Subst., o-Dekl.    -> decl_class='2nd'
  Subst., kons. Dekl.-> decl_class='3rd'
  Subst., u-Dekl.    -> decl_class='4th'
  Subst., e-Dekl.    -> decl_class='5th'

- Sobe confiança para 'conf:med' quando atualizado e conf ∈ {NULL, '', 'conf:low'}

Uso:
    python update_classes_from_latdeu_bs4.py --latdeu latdeu.db --retificado retificado_v2.db [--dry-run]

Requisitos:
    pip install beautifulsoup4
"""

import argparse
import sqlite3
import sys
import re
from bs4 import BeautifulSoup


def extract_span_g(html: str) -> str | None:
    """Extrai o texto do <span class="g">…</span>."""
    if not html:
        return None
    # parser nativo do Python é suficiente; se preferir, instale 'lxml' e use 'lxml'
    soup = BeautifulSoup(html, "html.parser")
    sp = soup.find("span", class_="g")
    if not sp:
        return None
    # get_text com espaço evita colagem de tokens
    return sp.get_text(" ", strip=True)


def parse_class(gram_raw: str) -> tuple[str | None, str | None]:
    """
    Recebe o texto interno do <span class="g">…</span> e devolve (kind, class).

    kind ∈ {'verb','adj','noun'} ou None
    class: 'a','e','i','cons' | 'a/o','3rd-2end','3rd-3end' | '1st'..'5th'
    """
    if not gram_raw:
        return (None, None, "")

    g = gram_raw.lower()

    # Normalizações simples
    g = g.replace("konj.", "konj.")  # já está em minúsculas
    g = g.replace("dekl.", "dekl.")
    g = g.replace("kons.", "kons.")
    g = re.sub(r"\s+", " ", g).strip()

    extra = ""

    if g.find("deponens") != -1:
        # deponens é uma forma verbal passiva que não tem ativo
        extra = " (deponens)"
        g = g.replace("deponens", "").strip()

    # Verbos
    if g.startswith("verb, a-konj"):
        return ("verb", "a", extra)
    if g.startswith("verb, e-konj"):
        return ("verb", "e", extra)
    if g.startswith("verb, i-konj"):
        return ("verb", "i", extra)
    # várias grafias que já vi por aí
    if g.startswith("verb, kons") or "verb" in g and "kons. konj" in g:
        return ("verb", "cons", extra)

    # Adjetivos
    if g.startswith("adj., a/o-dekl"):
        return ("adj", "a/o", extra)
    if "adj." in g and "mischkl." in g:
        if "2-endig" in g:
            return ("adj", "3rd-2end", extra)
        if "3-endig" in g:
            return ("adj", "3rd-3end", extra)
        # fallback genérico
        return ("adj", "3rd-2end", extra)

    # Substantivos (subst.)
    if g.startswith("subst., a-dekl"):
        return ("noun", "1st", extra)
    if g.startswith("subst., o-dekl"):
        return ("noun", "2nd", extra)
    if "subst." in g and "kons." in g:
        return ("noun", "3rd", extra)
    if g.startswith("subst., u-dekl"):
        return ("noun", "4th", extra)
    if g.startswith("subst., e-dekl"):
        return ("noun", "5th", extra)

    return (None, None, extra)


def derive_lemma_norm_from_latin(latin: str) -> str:
    """
    Deriva um lemma "norm" simples do campo VOC.latin:
    - pega só o trecho antes da primeira vírgula (padrão 'amāre, amō, amāvī, amātum')
    - lower()
    - NÃO normaliza u/v nem i/j (como no v2)
    """
    if not latin:
        return ""
    base = latin.split(",")[0].strip()
    return base.lower()


def ensure_indexes_retificado(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        CREATE INDEX IF NOT EXISTS idx_entry_lemma ON entry(lemma);
        CREATE INDEX IF NOT EXISTS idx_entry_lemma_sort ON entry(lemma_sort);
        CREATE INDEX IF NOT EXISTS idx_entry_conf ON entry(conf);
    """
    )


def bump_conf_if_needed(
    cur: sqlite3.Cursor, entry_id: int, lemma_norm_log: str, sets=[], params: list = []
) -> None:
    """
    Sobe confiança para 'conf:med' quando conf ∈ {NULL, '', 'conf:low'}.
    """
    cur.execute("SELECT conf FROM entry WHERE entry_id = ?", (entry_id,))
    row = cur.fetchone()
    if not row:
        return
    conf = row[0] if row[0] is not None else ""
    conf_norm = conf.strip().lower()
    print(
        f"[bump_conf] entry_id={entry_id}, conf='{conf_norm}', lemma_norm_log='{lemma_norm_log}', sets={sets}, params={params}"
    )
    if conf_norm in ("", "conf:low"):
        cur.execute(
            "UPDATE entry SET conf = 'conf:med' WHERE entry_id = ?", (entry_id,)
        )


def update_entry_classes(cur: sqlite3.Cursor, lemma_norm: str, payload: dict) -> int:
    """
    Atualiza classes na tabela entry:
    - casa por LOWER(lemma) ou LOWER(lemma_sort) == lemma_norm
    - retorna número de linhas afetadas
    """
    sets = []
    params = []
    if "verb_class" in payload:
        sets.append("verb_class = ?")
        params.append(payload["verb_class"])
    if "decl_class" in payload:
        sets.append("decl_class = ?")
        params.append(payload["decl_class"])
    if "adj_class" in payload:
        sets.append("adj_class = ?")
        params.append(payload["adj_class"])

    if not sets:
        return 0

    # atualiza e retorna IDs para eventual bump de confidência
    sql = f"""
        UPDATE entry
        SET {', '.join(sets)}
        WHERE LOWER(lemma) = ?
           OR LOWER(lemma_sort) = ?
        RETURNING entry_id
    """
    params.extend([lemma_norm, lemma_norm])
    cur.execute(sql, params)
    updated_ids = [r[0] for r in cur.fetchall()]
    for eid in updated_ids:
        bump_conf_if_needed(cur, eid, lemma_norm, sets, params)
    return len(updated_ids)


def collect_latdeu_classes(latdeu_db_path: str) -> dict[str, dict]:
    """
    Lê VOC do Lat→Deu e produz um dicionário:
      lemma_norm -> {'verb_class':..., 'decl_class':..., 'adj_class':...}
    """
    con = sqlite3.connect(latdeu_db_path)
    con.row_factory = sqlite3.Row
    classes: dict[str, dict] = {}

    for row in con.execute("SELECT latin, html FROM VOC"):
        html = row["html"]
        gram = extract_span_g(html)
        if not gram:
            continue
        kind, cls, extra = parse_class(gram)
        if not kind:
            continue

        lemma_norm = derive_lemma_norm_from_latin(row["latin"])
        if not lemma_norm:
            continue

        if extra:
            extra = f" ({extra})"

        print(f"[LATDEU] {lemma_norm} → {kind} class: {cls}{extra}")

        entry = classes.setdefault(lemma_norm, {})
        if kind == "verb":
            entry["verb_class"] = cls + extra
        elif kind == "adj":
            entry["adj_class"] = cls + extra
        elif kind == "noun":
            entry["decl_class"] = cls + extra

    con.close()
    return classes


def main():
    ap = argparse.ArgumentParser(
        description="Atualiza retificado_v2.db a partir do Lat→Deu (VOC.html) com BeautifulSoup."
    )
    ap.add_argument(
        "--latdeu", required=True, help="Caminho para o banco Lat→Deu (com tabela VOC)."
    )
    ap.add_argument(
        "--retificado", required=True, help="Caminho para retificado_v2.db."
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Não grava; apenas mostra contagem de alterações.",
    )
    args = ap.parse_args()

    classes = collect_latdeu_classes(args.latdeu)
    if not classes:
        print("Nenhuma classe extraída do Lat→Deu (VOC.html).", file=sys.stderr)

    con_rt = sqlite3.connect(args.retificado)
    con_rt.row_factory = sqlite3.Row
    ensure_indexes_retificado(con_rt)
    cur = con_rt.cursor()

    total_updates = 0
    for lemma_norm, payload in classes.items():
        total_updates += update_entry_classes(cur, lemma_norm, payload)

    if args.dry_run:
        con_rt.rollback()
        print(f"[dry-run] entradas atualizadas: {total_updates}")
    else:
        con_rt.commit()
        print(f"entradas atualizadas: {total_updates}")

    con_rt.close()


if __name__ == "__main__":
    main()
