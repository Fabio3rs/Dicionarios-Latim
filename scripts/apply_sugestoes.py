#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# apply_sugestoes.py — atualizado para usar "entry_id" quando presente
import argparse, json, re, sqlite3, sys, unicodedata
from datetime import datetime, timezone
from pathlib import Path

# ------------------------- util -------------------------

def strip_accents(s: str) -> str:
    return ''.join(ch for ch in unicodedata.normalize('NFD', s) if unicodedata.category(ch) != 'Mn')

def norm_lemma(s: str) -> str:
    return strip_accents(s or '').strip().lower()

def ascii_letters(s: str) -> str:
    return re.sub(r'[^A-Za-z]', '', strip_accents(s or ''))

def hyphen_agnostic(s: str) -> str:
    # remove hífens/pontos/prefixos separadores só para comparar forma-base
    return re.sub(r'[-·.\s]+', '', strip_accents(s or '')).lower()

def same_letters_only(old: str, new: str) -> bool:
    # “seguro” se a sequência de letras (ignorando acentos/hífens/caixa) é idêntica
    return ascii_letters(old).lower() == ascii_letters(new).lower()

def case_like(template: str, s: str) -> str:
    if not s: return s
    if template.isupper():
        return s.upper()
    if template.islower():
        return s.lower()
    # Titlecase (primeira maiúscula, resto como está no candidato)
    return s[0].upper() + s[1:]

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

# --------------------- POS heuristics --------------------

POS_MAP = {
    'v': 'verb', 'verb': 'verb',
    'adj': 'adj', 'a': 'adj',
    'adv': 'adv',
    'n': 'noun', 'subst': 'noun', 's': 'noun',
    'pron': 'pron', 'pronom': 'pron',
    'prep': 'prep', 'conj': 'conj', 'interj': 'interj',
}

MORPH_PATTERNS = {
    'verb': re.compile(r'\b(v\.|verbo|vb)\b', re.I),
    'adj':  re.compile(r'\b(adj\.?|adjetiv[oa])\b', re.I),
    'adv':  re.compile(r'\b(adv\.?|advérbi[oa])\b', re.I),
    'noun': re.compile(r'\b(n\.|s\.|subst|substantiv[oa])\b', re.I),
    'proper': re.compile(r'\b(pr\.|própr|propri[oa])\b', re.I),
    'pron': re.compile(r'\b(pron\.)\b', re.I),
    'prep': re.compile(r'\b(prep\.)\b', re.I),
    'conj': re.compile(r'\b(conj\.)\b', re.I),
    'interj': re.compile(r'\b(interj\.)\b', re.I),
}

def infer_pos_from_latdeu(hit):
    g = (hit or '').strip().lower()
    return POS_MAP.get(g)

def row_matches_pos(row_morf: str, want_pos: str) -> bool:
    if not want_pos: 
        return True
    pat = MORPH_PATTERNS.get(want_pos)
    if not pat:
        return True
    return bool(pat.search(row_morf or ''))

def is_proper(row_morf: str, lemma: str) -> bool:
    if MORPH_PATTERNS['proper'].search(row_morf or ''):
        return True
    return bool(lemma[:1].isupper())

# ----------------------- DB layer ------------------------

def fetch_by_id(conn, row_id):
    cur = conn.execute("""SELECT id, lema_canonico, lema_canonico_norm, morfologia, definicao, conf, notas
                          FROM entries WHERE id = ?""", (row_id,))
    r = cur.fetchone()
    if not r:
        return []
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r))]

def fetch_candidates(conn, forms):
    forms_norm = sorted(set(norm_lemma(x) for x in forms if (x or '').strip()))
    if not forms_norm:
        return []
    ph = ','.join('?' for _ in forms_norm)
    sql = f"""SELECT id, lema_canonico, lema_canonico_norm, morfologia, definicao, conf, notas
              FROM entries
              WHERE lema_canonico_norm IN ({ph})"""
    cur = conn.execute(sql, forms_norm)
    return [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]

def update_row(conn, row_id, new_lemma, conf_set, note_append, apply):
    if apply:
        conn.execute(
            """UPDATE entries
               SET lema_canonico = ?, conf = COALESCE(?, conf),
                   notas = CASE WHEN COALESCE(notas,'')='' THEN ? ELSE notas || CHAR(10) || ? END
               WHERE id = ?""",
            (new_lemma, conf_set, note_append, note_append, row_id)
        )

# ---------------------- core logic -----------------------

def process_record(rec, conn, conf_default, multi_mode, apply):
    lemma_in = rec.get('lemma_in') or rec.get('lemma') or ''
    entry_id = rec.get('entry_id') or rec.get('id')  # preferencial quando presente
    sug = (rec.get('suggestion') or {}) or {}
    lemma_sugerido = sug.get('lemma_sugerido') or rec.get('lemma_sugerido') or ''
    conf_set = (sug.get('confianca') or conf_default)
    motivo = (sug.get('motivo') or '').strip()
    variants = list(dict.fromkeys([lemma_in] + (rec.get('variants') or [])))

    if not lemma_in or not lemma_sugerido:
        print(f"[SKIP] campos insuficientes (lemma_in/lemma_sugerido/conf).")
        return

    # tentar POS pelos hits do latdeu
    latdeu_hits = rec.get('latdeu_hits') or []
    want_pos = None
    for h in latdeu_hits:
        want_pos = infer_pos_from_latdeu(h.get('grammar_note') or h.get('grammar'))
        if want_pos: break

    # --- alvo(s) ---
    if entry_id:
        cand_rows = fetch_by_id(conn, entry_id)
        if not cand_rows:
            print(f"[MISS] id não encontrado: '{entry_id}' (lemma_in='{lemma_in}').")
            return
        selected = cand_rows  # id é autoridade; ignora multi/resolução
        reason_tag = ["by-id"]
    else:
        cand_rows = fetch_candidates(conn, variants)
        if not cand_rows:
            print(f"[MISS] nenhum registro para '{lemma_in}'.")
            return

        # agrupar por lema_canonico_norm (apenas informativo)
        group_norm = sorted(set(r['lema_canonico_norm'] for r in cand_rows))
        if len(group_norm) > 1:
            print(f"[INFO] múltiplos norms mapeados por variantes: {', '.join(group_norm)}")

        selected = []
        reason_tag = []

        # filtro por POS quando disponível
        pos_filtered = [r for r in cand_rows if row_matches_pos(r.get('morfologia',''), want_pos)] if want_pos else cand_rows

        def safe_for_row(r):
            old = r['lema_canonico']
            new = lemma_sugerido
            if not same_letters_only(old, new):
                return False
            return True

        if len(cand_rows) > 1:
            print(f"[WARN] {len(cand_rows)} entradas para '{lemma_in}'.", end=' ')
            if want_pos and pos_filtered and len(pos_filtered) != len(cand_rows):
                print("Filtrando por classe morfológica inferida do Latin→German.")
                selected = pos_filtered
                reason_tag.append("pos-filter")
            else:
                if multi_mode == 'pos' and want_pos:
                    selected = pos_filtered
                    reason_tag.append("pos")
                elif multi_mode == 'safe':
                    selected = [r for r in cand_rows if safe_for_row(r)]
                    print("Aplicando apenas mudanças superficiais (acento/hífen/caixa).")
                    reason_tag.append("safe")
                elif multi_mode == 'one':
                    selected = [cand_rows[0]]
                    print("Atualizando apenas a 1ª ocorrência (modo 'one').")
                    reason_tag.append("one")
                else:  # 'all'
                    selected = cand_rows
                    print("Atualizando todas.")
                    reason_tag.append("all")
        else:
            selected = cand_rows

        if not selected:
            print(f"[REVIEW] '{lemma_in}' → '{lemma_sugerido}': multi sem alvo claro (POS={want_pos or 'n/d'}).")
            return

    # aplicar/emitir operações
    ts = utc_now_iso()
    base_note = f"[ocr_fix {ts}] {motivo or 'auto'} | lemma_in='{lemma_in}' → '{lemma_sugerido}'"
    for r in selected:
        old = r['lema_canonico']
        new_applied = case_like(old, lemma_sugerido)

        # guarda conservadora para nomes próprios só quando não há id explícito
        proper_mix = (entry_id is None) and is_proper(r.get('morfologia',''), old)
        if (entry_id is None) and len(selected) > 1 and proper_mix and not same_letters_only(old, lemma_sugerido):
            print(f"[REVIEW] {r['id']}: provável nome próprio/cognato '{old}'. Mantido. ({lemma_in}→{lemma_sugerido})")
            continue

        conf_final = conf_set if conf_set in ('conf:high','conf:med','conf:low') else None

        payload = {
            "action": "UPDATE",
            "id": r["id"],
            "old_lemma": old,
            "new_lemma": new_applied,
            "conf_set": conf_final or r.get("conf"),
            "def_changed": False,
            "note": base_note
        }
        print(json.dumps(payload, ensure_ascii=False))
        update_row(conn, r["id"], new_applied, conf_final, base_note, apply)


def unaccent_lower(s):
    if s is None:
        return None
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.casefold()

# ------------------------- CLI --------------------------

def main():
    ap = argparse.ArgumentParser(description="Aplica sugestões de lemas no retificado.db; usa entry_id se presente.")
    ap.add_argument("--db", required=True, help="caminho do retificado.db")
    ap.add_argument("--jsonl", required=True, help="arquivo de sugestões (JSONL)")
    ap.add_argument("--apply", action="store_true", help="sem esta flag, faz só dry-run")
    ap.add_argument("--conf-default", default="conf:high", help="conf a aplicar quando não vier no JSONL")
    ap.add_argument("--multi-mode", default="safe", choices=["safe","pos","all","one"],
                    help="estratégia p/ multis sem id: safe (padrão), pos, all, one")
    args = ap.parse_args()

    db = sqlite3.connect(args.db)
    db.execute("PRAGMA journal_mode=WAL;")
    db.execute("PRAGMA synchronous=NORMAL;")
    db.create_function("UNACCENT_LOWER", 1, unaccent_lower, deterministic=True)

    try:
        if args.apply:
            db.execute("BEGIN;")
        with open(args.jsonl, "r", encoding="utf-8") as fh:
            for ln, line in enumerate(fh, 1):
                line = line.strip()
                if not line: 
                    continue
                try:
                    rec = json.loads(line)
                except Exception as e:
                    print(f"[SKIP] L{ln}: JSON inválido: {e}")
                    continue
                process_record(rec, db, args.conf_default, args.multi_mode, args.apply)
        if args.apply:
            db.commit()
            print("[OK] alterações gravadas.")
        else:
            print("[DRY-RUN] nenhuma alteração gravada (use --apply).")
    except Exception as e:
        if args.apply:
            db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
