
# Lexicon (SQLite) — Guia rápido

Usamos o SQLite como entrega final, mas tratamos o `.db` como derivado:
- ✅ **Commitar:** `schema_normalizado.sql`, `resultados/normalized_results_v2.json`, scripts (`ingest_normalized.py`, `query_lexicon.py`, `export_normalized_from_retificado_v2.py`), e **exports determinísticos** (`exports/*.jsonl`).
- ⚠️ **Não commitar:** `lexicon.db` (binário) — publique em release/LFS se precisar.

## Fluxo sugerido
1. Gere o JSON oficial a partir do banco canônico:
   ```bash
   make data-export-v2  # dicionarios/retificado_v2.db -> resultados/normalized_results_v2.json
   ```
2. Construa o `lexicon.db` (local):
   ```bash
   python ingest_normalized.py --json resultados/normalized_results_v2.json --db resultados/lexicon.db --schema scripts/schema_normalizado.sql --batch-fts
   ```
3. Valide e gere exports determinísticos (bons para diff em PRs):
   ```bash
   python scripts/export_for_diff.py --db lexicon.db --out-dir exports
   ```
4. Commit: `schema_normalizado.sql`, `resultados/normalized_results_v2.json`, `exports/*.jsonl`. O `lexicon.db` fica fora do Git.

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
