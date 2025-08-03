
# Lexicon (SQLite) — Repo Guide

Este repositório armazena **fontes textuais** (JSON/SQL) e scripts; o `.db` pode ser opcional:
- ✅ **Commitar:** `schema_normalizado.sql`, `normalized_results.json`, scripts (`ingest_normalized.py`, `query_lexicon.py`), e **exports determinísticos** (`exports/*.jsonl`).
- ⚠️ **Opcional (ou via LFS):** `lexicon.db` (binário). Se o DB passar de 100 MB, use Git LFS ou publique como artefato de release.

## Fluxo sugerido
1. Edite `normalized_results.json` e rode:
   ```bash
   python ingest_normalized.py --json normalized_results.json --db lexicon.db --schema schema_normalizado.sql --batch-fts
   ```
2. Valide e gere exports determinísticos (bons para diff em PRs):
   ```bash
   python scripts/export_for_diff.py --db lexicon.db --out-dir exports
   ```
3. Commit: `schema_normalizado.sql`, `normalized_results.json`, `exports/*.jsonl`. O `lexicon.db` pode ficar fora do Git (ou ir para LFS).

## Por que exports determinísticos?
Bins (`.db`) não fazem diff legível. Mantemos **JSONL ordenado** com chaves estáveis:
- `exports/entry.jsonl`: uma linha por `entry` + lemas agregados
- `exports/exemplo.jsonl`: uma linha por exemplo com `parse_status`, `confidence`, etc.

## Boas práticas
- Ignore `-wal`/`-shm` (já no `.gitignore`).
- `VACUUM` antes de publicar releases de `.db`.
- Use tags/releases para versões imutáveis (ex.: `v0.3.0`).
- Se usar LFS, habilite no repositório e descomente as linhas do `.gitattributes`.

## CI
O workflow `Lexicon CI`:
- reconstrói `lexicon.db` a partir do JSON,
- roda `PRAGMA integrity_check`,
- publica os exports determinísticos como artifact no PR.

## Ferramentas
- `ingest_normalized.py` — ingere o JSON e constrói o DB.
- `query_lexicon.py` — CLI de consulta com FTS5.
- `scripts/export_for_diff.py` — gera `exports/*.jsonl` com ordenação estável.
