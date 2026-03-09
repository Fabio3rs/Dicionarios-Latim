#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dedupe_apply_safe.py
--------------------
Valida e aplica (ou prepara) decisões vindas do OpenAI Batch para deduplicação de entradas
no "retificado.db" (tabela "entries"), **com checagens fortes** contra alucinações:

Checagens por grupo (custom_id = "dedupe::<lema_canonico_norm>"):
  - O grupo existe no DB e tem >= min_group_size
  - N-entra/N-sai: quantidade de "results" == quantidade de IDs do grupo
  - Conjunto de IDs emitidos == conjunto de IDs do grupo (sem sobras/faltas)
  - redirect_to (quando houver) aponta **para ID do mesmo grupo**
  - Ação "manual" é ignorada por padrão (não aplicar)
  - Campos opcionais "fix_*" são ignorados aqui (sem tocar definição/lemma)

Saídas:
  - decisions_safe.jsonl : somente itens válidos, mapeados para o formato do dedupe_apply.py:
      {"id": "...", "keep": true}
      {"id": "...", "keep": false, "redirect_to": "..."}
  - rejects.jsonl        : itens/explanações recusados (inseguros/invalidos)
  - Um resumo no stdout

Uso:
  python3 dedupe_apply_safe.py \
    --retificado retificado.db \
    --batch-output batches_dedupe_output.jsonl \
    --out decisions_safe.jsonl \
    --rejects rejects.jsonl \
    [--min-group-size 2] \
    [--strict] \
    [--skip-manual] \
    [--run-apply dedupe_apply.py] \
    [--dry-run]

Se --run-apply for fornecido, o script chamará o aplicador existente como subprocesso:
  python3 dedupe_apply.py --retificado-db retificado.db --decisions-jsonl decisions_safe.jsonl
Você pode acrescentar flags extras para o aplicador após um "--" (duplo hífen).
"""
from __future__ import annotations
import argparse, json, os, sqlite3, sys, subprocess, shlex
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import unicodedata
import sys
import os

def log(msg: str):
    print(msg, file=sys.stderr)

def open_sqlite(path: str) -> sqlite3.Connection:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con

def load_group_ids(con: sqlite3.Connection, lemma_norm: str) -> List[str]:
    q = """SELECT id FROM entries WHERE lema_canonico_norm = ? ORDER BY id"""
    cur = con.execute(q, (lemma_norm,))
    return [r[0] for r in cur.fetchall()]

def parse_batch_output_line(obj: dict) -> Tuple[Optional[str], List[dict], Optional[str]]:
    """
    Retorna (group, results[], error_msg)
    - group extraído de custom_id "dedupe::<group>"
    - results: da resposta do modelo (parsed se disponível; senão parse do content)
    - error_msg: se não conseguir extrair
    """
    try:
        cid = obj.get("custom_id") or ""
        group = None
        if cid.startswith("dedupe::"):
            group = cid.split("dedupe::", 1)[1]
        rsp = obj.get("response") or {}
        body = rsp.get("body") or rsp.get("output") or {}
        choices = (body.get("choices") or [])
        if not choices:
            return (group, [], "no choices in body")
        msg = (choices[0] or {}).get("message") or {}
        # Prefer "parsed" (Structured Outputs)
        if "parsed" in msg and isinstance(msg["parsed"], dict):
            results = msg["parsed"].get("results") or []
            if isinstance(results, list):
                return (group, results, None)
        # Fallback: parse content
        content = msg.get("content") or ""
        if isinstance(content, str) and content.strip():
            try:
                parsed = json.loads(content)
                if isinstance(parsed, dict) and isinstance(parsed.get("results"), list):
                    return (group, parsed["results"], None)
            except Exception:
                return (group, [], "content not JSON or lacks results")
        return (group, [], "no parsed results")
    except Exception as e:
        return (None, [], f"exception parsing line: {e}")

def validate_group(group: str,
                   group_ids_db: List[str],
                   results: List[dict],
                   strict: bool = True,
                   skip_manual: bool = True) -> Tuple[List[dict], List[dict]]:
    """
    Retorna (valid_items, rejects). Cada item válido é convertido para o formato do dedupe_apply.py.
    Rejects trazem {"group","item","reason"}.
    """
    rejects = []
    valid = []

    if not group_ids_db:
        rejects.append({"group": group, "item": None, "reason": "group_not_in_db"})
        return ([], rejects)

    # Coletar ids do output e checar cardinalidade/conjunto
    out_ids = []
    for it in results:
        iid = str(it.get("id") or "")
        if iid:
            out_ids.append(iid)
    if strict:
        if len(out_ids) != len(group_ids_db):
            rejects.append({"group": group, "item": None, "reason": f"count_mismatch out={len(out_ids)} db={len(group_ids_db)}"})
            # mesmo assim, vamos continuar e filtrar o que der
        if set(out_ids) != set(group_ids_db):
            rejects.append({"group": group, "item": None, "reason": "id_set_mismatch"})

    group_set = set(group_ids_db)

    for it in results:
        iid = str(it.get("id") or "")
        if not iid:
            rejects.append({"group": group, "item": it, "reason": "missing_id"})
            continue
        if iid not in group_set:
            rejects.append({"group": group, "item": it, "reason": "id_not_in_group"})
            continue

        action = (it.get("action") or "").strip().lower()
        redirect_to = it.get("redirect_to")
        if isinstance(redirect_to, str) and redirect_to.strip() == "":
            redirect_to = None

        # skip_manual: não aplicar "manual"
        if action == "manual":
            if skip_manual:
                # apenas rejeita silenciosamente com anotação
                rejects.append({"group": group, "item": {"id": iid}, "reason": "manual_skipped"})
                continue
            # se não pular, trataria como no-op (keep?) mas é arriscado -> rejeitar
            rejects.append({"group": group, "item": {"id": iid}, "reason": "manual_unsupported"})
            continue

        if action not in ("keep", "redirect"):
            rejects.append({"group": group, "item": it, "reason": f"unknown_action:{action}"})
            continue

        if action == "keep":
            valid.append({"id": iid, "keep": True})
            continue

        # action == redirect
        if not redirect_to:
            rejects.append({"group": group, "item": it, "reason": "redirect_missing_target"})
            continue
        if redirect_to not in group_set:
            rejects.append({"group": group, "item": it, "reason": "redirect_cross_group_or_invalid"})
            continue
        if redirect_to == iid:
            rejects.append({"group": group, "item": it, "reason": "redirect_to_self"})
            continue

        valid.append({"id": iid, "keep": False, "redirect_to": redirect_to})

    return (valid, rejects)


def unaccent_lower(s):
    if s is None:
        return None
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.casefold()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--retificado", required=True, help="retificado.db (SQLite)")
    ap.add_argument("--batch-output", required=True, help="batches_dedupe_output.jsonl (da OpenAI Batch API)")
    ap.add_argument("--out", default="decisions_safe.jsonl", help="arquivo JSONL de decisões válidas")
    ap.add_argument("--rejects", default="rejects.jsonl", help="arquivo JSONL de rejeições/erros")
    ap.add_argument("--min-group-size", type=int, default=2, help="ignorar grupos com menos que este tamanho")
    ap.add_argument("--strict", action="store_true", help="falha/grava reject quando cardinalidade ou id_set divergir")
    ap.add_argument("--skip-manual", action="store_true", help="ignorar itens com action=manual (padrão)")
    ap.add_argument("--run-apply", metavar="APPLY_SCRIPT", help="chamar aplicador existente (ex.: dedupe_apply.py) após gerar decisions_safe.jsonl")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--", dest="apply_args", nargs=argparse.REMAINDER, help="args extras para o aplicador após --")
    args = ap.parse_args()

    con = open_sqlite(args.retificado)
    con.create_function("UNACCENT_LOWER", 1, unaccent_lower, deterministic=True)

    total_groups = 0
    groups_ok = 0
    total_items = 0
    kept = 0
    redirects = 0
    manuals = 0

    out_f = open(args.out, "w", encoding="utf-8")
    rej_f = open(args.rejects, "w", encoding="utf-8")

    with open(args.batch_output, "r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                rej_f.write(json.dumps({"_line": ln, "reason": "invalid_json"}) + "\n")
                continue

            # extrai grupo e results
            group, results, err = parse_batch_output_line(obj)
            if not group:
                rej_f.write(json.dumps({"_line": ln, "reason": "missing_or_invalid_custom_id"}) + "\n")
                continue

            group_ids = load_group_ids(con, group)

            if len(group_ids) < args.min_group_size:
                # provavelmente não é um grupo de duplicatas, ignore
                rej_f.write(json.dumps({"group": group, "reason": "group_below_min_size", "size": len(group_ids)}) + "\n")
                continue

            total_groups += 1
            total_items += len(group_ids)

            if err:
                rej_f.write(json.dumps({"group": group, "reason": f"parse_error:{err}"}) + "\n")
                continue

            valid, rejects = validate_group(group, group_ids, results, strict=args.strict, skip_manual=args.skip_manual)

            # estatísticas locais
            for it in results:
                if (it.get("action") or "").lower() == "manual":
                    manuals += 1
            kept += sum(1 for v in valid if v.get("keep") is True)
            redirects += sum(1 for v in valid if v.get("keep") is False)

            # escreve rejects
            for r in rejects:
                rej_f.write(json.dumps(r, ensure_ascii=False) + "\n")

            # se strict e houver mismatch grave, não emite nada para o grupo
            if args.strict and any(r.get("reason") in ("count_mismatch","id_set_mismatch") for r in rejects):
                continue

            # escreve válidos
            for v in valid:
                out_f.write(json.dumps(v, ensure_ascii=False) + "\n")
            groups_ok += 1

    out_f.close()
    rej_f.close()

    # resumo
    print(f"[SUMMARY] groups_seen={total_groups} groups_ok={groups_ok} items_in_groups={total_items} kept={kept} redirects={redirects} manuals_seen={manuals}")
    print(f"[FILES] decisions={args.out} rejects={args.rejects}")

    # aplicar (opcional)
    if args.run_apply and not args.dry_run:
        cmd = [sys.executable, args.run_apply, "--retificado-db", args.retificado, "--decisions-jsonl", args.out]
        if args.apply_args:
            # remove leading "--" if present
            extra = [x for x in args.apply_args if x != "--"]
            cmd.extend(extra)
        log("[APPLY] " + " ".join(shlex.quote(c) for c in cmd))
        rc = subprocess.call(cmd)
        if rc != 0:
            log(f"[APPLY] aplicador retornou código {rc}")
            sys.exit(rc)

if __name__ == "__main__":
    main()
