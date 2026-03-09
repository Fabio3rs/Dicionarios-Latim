#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dedupe_batch_prep.py
--------------------
Prepara um JSONL por *grupo* de duplicatas (mesmo lema_canonico_norm) para uso na API de Batching
da OpenAI (/v1/chat/completions), com *pistas* (LS/LatDeu/Whitaker/Gaffiot) opcionais.

Saídas:
- ./batches_dedupe/<lemma_norm_sanitizado>.jsonl  (um request /v1/chat/completions por grupo)
- ./batches_dedupe/manifest.csv                   (mapa de grupos → arquivo/n_itens)

Cada JSONL contém uma ÚNICA linha "request" (um objeto), com body estruturado:
- messages[0] = SYSTEM (prompt consolidado com regras N-entra/N-sai, etc.)
- messages[1] = USER    (blocos de "pistas" + lista JSON de entradas do grupo)
- response_format = json_schema (schema de saída "results[]", 1 objeto por id de entrada)

Uso (exemplo mínimo):
  python3 dedupe_batch_prep.py \
    --retificado retificado.db \
    --outdir batches_dedupe

Com evidências externas:
  python3 dedupe_batch_prep.py \
    --retificado retificado.db \
    --ls ls_dict.sqlite \
    --latdeu token_latim_german.sqlite \
    --gaffiot gaffiot.db \
    --whitaker bin/words \
    --outdir batches_dedupe

Parâmetros adicionais:
  --limit-groups 200            # limitar número de grupos (debug)
  --min-dups 2                  # tamanho mínimo do grupo (default=2)
  --model gpt-5-mini            # nome do modelo a ser usado pelo batch
  --reasoning medium            # low|medium|high (reasoning_effort)
  --max-items 200               # teto de itens/dicionário nas "pistas" (por segurança)
  --quiet                       # menos logs

Requisitos:
  - Python 3.9+
  - sqlite3, json, argparse
  - (opcional) sqlite dos dicionários externos; binário do Whitaker se desejar pistas

Observações:
  - O arquivo gerado é *diretamente* utilizável no fluxo de Batches:
      (1) upload → (2) batch create → (3) aguardar → (4) download out.jsonl
  - O contrato de saída é verificado posteriormente por seu aplicador (dedupe_apply.py).
"""
from __future__ import annotations
import argparse, json, os, re, sqlite3, sys, csv
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import unicodedata

# ----------------------------
# Utilidades
# ----------------------------


def strip_accents(s: str) -> str:
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


def log(msg: str, quiet: bool = False):
    if not quiet:
        print(msg, file=sys.stderr)


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def sanitize_filename(s: str) -> str:
    s = s or "group"
    s = s.lower()
    s = re.sub(r"[^a-z0-9._-]+", "_", s)
    return s[:200] or "group"


# Normalização latina consistente com seus scripts
def norm_latin_basic(s: str) -> str:
    s = strip_accents(s or "").lower()
    s = s.replace("(", " ").replace(")", " ")
    s = s.replace("'", " ").replace('"', " ")
    # aproximação leve (não remove acentos aqui; DB já possui *_norm)
    # s = s.replace("j", "i").replace("v", "u")
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ----------------------------
# Consultas externas (pistas)
# ----------------------------


def open_sqlite(path: Optional[str]) -> Optional[sqlite3.Connection]:
    if not path:
        return None
    if not os.path.exists(path):
        return None
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def ls_pistas(
    conn: Optional[sqlite3.Connection], lemas: List[str], max_items: int = 100
) -> str:
    """Retorna um texto curto com pistas do LS para o conjunto de lemas."""
    if not conn or not lemas:
        return ""
    cand = [norm_latin_basic(x) for x in lemas if x]
    cand = sorted(set(cand))
    placeholders = ",".join("?" for _ in cand)
    placeholders2 = ",".join("?" for _ in lemas)
    try:
        q = f"""
        SELECT lemma, lemma_norm, pos, itypes, definition, tr_gloss_pt
        FROM ls_entries
        WHERE lemma_norm IN ({placeholders}) OR lemma IN ({placeholders2})
        LIMIT {max_items}
        """
        cur = conn.execute(q, tuple(cand + lemas))
        rows = cur.fetchall()
    except Exception as e:
        log(f"[ERROR] LS query failed: {e}", quiet=False)
        return ""
    out = []
    for r in rows[:max_items]:
        pos = (r["pos"] or "").strip()
        it = (r["itypes"] or "").strip()
        defi = (r["tr_gloss_pt"]).strip() + " " + (r["definition"] or "").strip()
        line = f"- {r['lemma']}  pos={pos or '-'}  itypes={it or '-'}  def={defi[:250].strip()}"
        out.append(line)
    return "\n".join(out)


def latdeu_pistas(
    latdeu_db: Optional[str], lemas: List[str], max_items: int = 100
) -> str:
    if not latdeu_db or not os.path.exists(latdeu_db) or not lemas:
        return ""
    try:
        conn = sqlite3.connect(latdeu_db)
        conn.row_factory = sqlite3.Row
        cand = [norm_latin_basic(x) for x in lemas if x]
        cand = sorted(set(cand))
        placeholders = ",".join("?" for _ in cand)
        q = f"""
        SELECT F.form, V.latin, V."desc" AS descr, V.grammar AS grammar_note
        FROM FORM AS F
        JOIN VOC  AS V ON V.vok_id = F.vok_id
        WHERE F.form_norm IN ({placeholders})
        LIMIT {max_items}
        """
        cur = conn.execute(q, tuple(cand))
        rows = cur.fetchall()
    except Exception:
        return ""
    out = []
    for r in rows[:max_items]:
        line = f"- {r['form']} ⇒ {r['latin']}  {r['descr'] or ''}  [{r['grammar_note'] or ''}]"
        out.append(line.strip())
    return "\n".join(out)


def gaffiot_pistas(
    gaffiot_db: Optional[str], lemas: List[str], max_items: int = 100
) -> str:
    if not gaffiot_db or not os.path.exists(gaffiot_db) or not lemas:
        return ""
    try:
        conn = sqlite3.connect(gaffiot_db)
        conn.row_factory = sqlite3.Row
        cand = [norm_latin_basic(x) for x in lemas if x]
        cand = sorted(set(cand))
        placeholders = ",".join("?" for _ in cand)
        placeholders2 = ",".join("?" for _ in lemas)
        # esquema comum do build_gaffiot_sqlite.py
        q = f"""
        SELECT lemma, pos, itype, gen_text, head_raw AS definition
        FROM entry
        WHERE lemma_sort IN ({placeholders}) OR lemma IN ({placeholders2})
        LIMIT {max_items}
        """
        cur = conn.execute(q, tuple(cand + lemas))
        rows = cur.fetchall()
    except Exception as e:
        log(f"[ERROR] Gaffiot query failed: {e}", quiet=False)
        return ""
    out = []
    for r in rows[:max_items]:
        pos = (r["pos"] or "").strip()
        defi = (r["definition"] or "").strip()
        out.append(f"- {r['lemma']}  pos={pos or '-'}  def={defi[:160]}")
    return "\n".join(out)


def whitaker_pistas(
    whitaker_bin: Optional[str], lemas: List[str], max_items: int = 50
) -> str:
    """
    Executa o binário do Whitaker (se existir). Para manter simples e robusto,
    só concatena primeiras linhas que parecem "de dicionário".
    """
    if (
        not whitaker_bin
        or not os.path.exists(f"whitakers-words/{whitaker_bin}")
        or not lemas
    ):
        return ""
    # Evitar execuções pesadas: limitar número de lemas
    import subprocess, shlex

    lemas = sorted(set(map(norm_latin_basic, set(lemas))))

    tostdin = "\n" * (len(lemas) + 1)  # simular ENTERs para evitar prompts

    proc = subprocess.run(
        [whitaker_bin, *lemas],
        capture_output=True,
        text=True,
        input=tostdin,
        cwd="whitakers-words",
    )
    txt = proc.stdout.strip()

    result_stdout = "\n".join(
        line for line in txt.splitlines() if "UNKNOWN" not in line
    )
    # Heurística simples: pegar linhas com ' ' e ';' (formato típico)
    return result_stdout.strip()


# ----------------------------
# Prompts
# ----------------------------

SYSTEM_PROMPT = """Você é um lexicógrafo digital. Sua tarefa: resolver duplicatas por grafia normalizada,
mantendo homônimos distintos e aplicando **N-entra/N-sai** (para cada ID de entrada, produza exatamente UMA decisão).
Use as evidências (LS, Lat→Deu, Whitaker, Gaffiot) de forma conservadora. Prefira redirecionar a deletar.

Ações possíveis por item:
- keep               → manter o registro como canônico para o seu sentido
- redirect           → redirecionar para outro ID **do mesmo grupo** (quando for mera duplicata)
- manual             → marcar para revisão manual quando faltar evidência ou houver conflito forte

Políticas:
- Homônimos (POS/sentido distintos) → mantenha **>1 keeps** e distribua definições coerentes.
- Se detectar **swap** de definições, realoque os textos entre IDs corretos (e explique em reasons).
- Ortografia: corrija apenas aspectos superficiais (maiúsculas/acentos/hífens) quando houver consenso nas fontes.
- Evidência mínima para KEEP: >= 1 fonte externa sem conflito; para HIGH, >= 2 fontes concordantes.
- Nunca referencie IDs fora do grupo no campo redirect_to.

Output estrito via Structured Output (JSON Schema). **NÃO** escreva nada fora do JSON final.
"""

# JSON Schema para Structured Outputs (OpenAI responses/ chat.completions)
# O chamador (batch) vai usar: {"response_format":{"type":"json_schema","json_schema":{...}}}
RESULTS_JSON_SCHEMA = {
    "name": "dedupe_results",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["id", "action", "reasons"],
                    "properties": {
                        "id": {"type": "string"},
                        "action": {
                            "type": "string",
                            "enum": ["keep", "redirect", "manual"],
                        },
                        "redirect_to": {"type": ["string", "null"]},
                        "canonical_group": {"type": ["string", "null"]},
                        "fix_lemma": {"type": ["string", "null"]},
                        "fix_morfologia": {"type": ["string", "null"]},
                        "fix_definicao": {"type": ["string", "null"]},
                        "reasons": {"type": "array", "items": {"type": "string"}},
                    },
                },
            }
        },
        "required": ["results"],
    },
}


def build_user_prompt_block(
    lema_norm: str,
    group_rows: List[sqlite3.Row],
    pistas_ls: str,
    pistas_latdeu: str,
    pistas_whit: str,
    pistas_gaffiot: str,
) -> str:
    lemas = sorted(
        {
            (r["lema_canonico"] or "").strip()
            for r in group_rows
            if (r["lema_canonico"] or "").strip()
        }
    )
    header = [
        f"# GRUPO: {lema_norm}",
        "",
        "## EVIDÊNCIAS (resumo)",
    ]
    if pistas_ls:
        header += ["[LS]", pistas_ls, ""]
    if pistas_latdeu:
        header += ["[Lat→Deu]", pistas_latdeu, ""]
    if pistas_whit:
        header += ["[Whitaker]", pistas_whit, ""]
    if pistas_gaffiot:
        header += ["[Gaffiot]", pistas_gaffiot, ""]

    header += [
        "## ITENS DO GRUPO (JSON, ordem fixa; N-entra/N-sai)",
        # listagem json dos itens do grupo (um array; o modelo vai refletir 1:1 no results[])
    ]

    items = []
    for r in group_rows:
        items.append(
            {
                "id": r["id"],
                "lemma": r["lema_canonico"],
                "morfologia": r["morfologia"],
                "definicao": r["definicao"],
                "conf": r["conf"],
            }
        )

    body = json.dumps({"items": items}, ensure_ascii=False)
    return "\n".join(header) + "\n" + body


# ----------------------------
# Núcleo: geração de batches
# ----------------------------


def fetch_duplicate_groups(
    conn: sqlite3.Connection, min_dups: int = 2, limit_groups: Optional[int] = None
) -> List[str]:
    q = f"""
    SELECT lema_canonico_norm
    FROM entries
    GROUP BY lema_canonico_norm
    HAVING COUNT(*) >= ?
    ORDER BY COUNT(*) DESC, lema_canonico_norm
    """
    if limit_groups:
        q += " LIMIT ?"
        rows = conn.execute(q, (min_dups, limit_groups)).fetchall()
    else:
        rows = conn.execute(q, (min_dups,)).fetchall()
    return [r[0] for r in rows if r[0]]


def fetch_group_rows(conn: sqlite3.Connection, lema_norm: str) -> List[sqlite3.Row]:
    q = """
    SELECT id, lema_canonico, morfologia, definicao, conf
    FROM entries
    WHERE lema_canonico_norm = ?
    ORDER BY id
    """
    cur = conn.execute(q, (lema_norm,))
    return cur.fetchall()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--retificado", required=True, help="SQLite retificado.db")
    ap.add_argument("--ls", help="SQLite ls_dict.sqlite")
    ap.add_argument("--latdeu", help="SQLite token_latim_german.sqlite")
    ap.add_argument("--gaffiot", help="SQLite gaffiot.db")
    ap.add_argument("--whitaker", help="Caminho do binário Whitaker (opcional)")
    ap.add_argument("--outdir", required=True, help="Diretório de saída dos JSONLs")
    ap.add_argument(
        "--limit-groups", type=int, help="Limitar quantidade de grupos (debug)"
    )
    ap.add_argument("--min-dups", type=int, default=2, help="Tamanho mínimo do grupo")
    ap.add_argument("--model", default="gpt-5-mini", help="Modelo a ser usado no batch")
    ap.add_argument(
        "--reasoning",
        default="medium",
        choices=["low", "medium", "high"],
        help="reasoning_effort",
    )
    ap.add_argument(
        "--max-items",
        type=int,
        default=200,
        help="Teto de registros por dicionário nas pistas",
    )
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    ensure_dir(outdir)
    manifest_path = outdir / "manifest.csv"

    con_ret = sqlite3.connect(args.retificado)
    con_ret.row_factory = sqlite3.Row

    con_ls = open_sqlite(args.ls)
    # latdeu usa conexão ad-hoc (fechada após uso)
    gaff_db = args.gaffiot  # abrir sob demanda
    whit_bin = args.whitaker

    groups = fetch_duplicate_groups(
        con_ret, min_dups=args.min_dups, limit_groups=args.limit_groups
    )
    log(f"[INFO] grupos duplicados: {len(groups)}", args.quiet)

    batchfile = open(outdir / "batches_dedupe.jsonl", "w", encoding="utf-8")

    with open(manifest_path, "w", newline="", encoding="utf-8") as mf:
        w = csv.writer(mf, delimiter="\t")
        w.writerow(["group", "file", "n_items"])

        for gi, g in enumerate(groups, 1):
            rows = fetch_group_rows(con_ret, g)
            if not rows:
                continue

            # montar "pistas"
            lemas = [
                (r["lema_canonico"] or "").strip()
                for r in rows
                if (r["lema_canonico"] or "").strip()
            ]
            pistas_ls = ls_pistas(con_ls, lemas, args.max_items)
            pistas_lat = latdeu_pistas(args.latdeu, lemas, args.max_items)
            pistas_gaf = gaffiot_pistas(gaff_db, lemas, args.max_items)
            pistas_whi = whitaker_pistas(
                whit_bin, lemas, max_items=min(30, args.max_items)
            )

            user_block = build_user_prompt_block(
                g, rows, pistas_ls, pistas_lat, pistas_whi, pistas_gaf
            )

            if not args.quiet:
                print(
                    f"[INFO] Processando grupo {gi}/{len(groups)}: {g} (n={len(rows)})"
                )
                print(f"  LS: {len(pistas_ls)} chars, LatDeu:\n{pistas_lat}\n")
                print(f"  Gaffiot: {len(pistas_gaf)} chars, Whitaker:\n{pistas_whi}\n")
                print(f"  User block size: {len(user_block)} chars;\n{user_block}\n\n")
                print(f"  Total items: {len(rows)}")

            # request para batch (uma linha JSON por arquivo)
            body = {
                "model": args.model,
                "reasoning_effort": args.reasoning,
                "presence_penalty": 0.0,
                "frequency_penalty": 0.0,
                "response_format": {
                    "type": "json_schema",
                    "json_schema": RESULTS_JSON_SCHEMA,
                },
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_block},
                ],
            }
            request_obj = {
                "custom_id": f"dedupe::{g}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": body,
            }

            # salvar
            # fname = f"{sanitize_filename(g)}.jsonl"
            # fpath = outdir / fname

            batchfile.write(json.dumps(request_obj, ensure_ascii=False) + "\n")
            # with open(fpath, "w", encoding="utf-8") as out:
            #     out.write(json.dumps(request_obj, ensure_ascii=False) + "\n")

            # w.writerow([g, str(fpath), len(rows)])
            # log(f"[OK] grupo {gi}/{len(groups)} → {fname} (n={len(rows)})", args.quiet)

    log(f"[DONE] Manifesto em: {manifest_path}", args.quiet)


if __name__ == "__main__":
    main()
