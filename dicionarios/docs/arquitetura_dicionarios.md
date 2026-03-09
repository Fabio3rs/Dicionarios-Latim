# Arquitetura de Dicionários Latinos — SQLite como Fonte da Verdade

## Filosofia

- **SQLite = fonte primária**: todos os dicionários em bancos relacionais com schema consistente
- **Exportações (.md, .html, .json)**: geradas sob demanda a partir dos SQLites
- **Versionamento**: timestamps + git para rastrear mudanças no schema
- **Interoperabilidade**: schemas compatíveis para queries cross-dictionary

---

## Bancos Existentes

### 1. `retificado_v2.db` — Dicionário Faria Corrigido (34.8k entradas)

**Schema**:
```sql
CREATE TABLE entry (
  id              INTEGER PRIMARY KEY,
  lemma           TEXT NOT NULL,      -- forma canônica (a, ab, abacus)
  morph_render    TEXT,               -- s. masc. 2ª (-i), v. 1ª (-āre)
  definicao       TEXT,               -- texto da definição PT
  notas           TEXT,               -- observações etimológicas
  conf            TEXT,               -- high/med/low (confiança da correção OCR)
  needs_review    INTEGER DEFAULT 0,
  redirect_only   INTEGER DEFAULT 0,
  morph_out_of_vocab INTEGER DEFAULT 0,
  raw_json        TEXT                -- JSON original do LLM
);
```

**Uso**: Dicionário Latin→PT de Ernesto Faria digitalizado e corrigido.

---

### 2. `ls_dict.db` — Lewis & Short Traduzido (42k entradas)

**Schema**:
```sql
CREATE TABLE source (
  source_id   INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  edition     TEXT,
  license     TEXT,
  uri         TEXT
);

CREATE TABLE entry (
  id                TEXT UNIQUE,          -- perseus:latin:n1234
  entry_id          INTEGER PRIMARY KEY,
  source_id         INTEGER REFERENCES source(source_id),
  lemma             TEXT NOT NULL,
  lemma_sort        TEXT NOT NULL,        -- normalizado para ordenação
  homograph_no      INTEGER,              -- distingue homógrafos (a¹, a²)
  pos               TEXT,                 -- n., v., adj., adv., prep.
  gen_text          TEXT,                 -- gen., acc., abl. (para prep.)
  indeclinable      INTEGER DEFAULT 0,
  itype             TEXT,                 -- classe morfológica (conj1, decl2)
  itype_json        TEXT,                 -- JSON array de tokens morfo
  head_raw          TEXT,                 -- linha de cabeçalho original
  xml_fragment      TEXT NOT NULL,        -- XML TEI do Perseus
  -- Campos de tradução PT-BR (adicionados por você):
  tr_gloss_pt       TEXT DEFAULT '',      -- glosas curtas PT
  tr_trad_pt        TEXT DEFAULT '',      -- tradução da definição completa
  tr_notas          TEXT DEFAULT '',      -- notas do tradutor
  tr_citacoes       TEXT DEFAULT '[]',    -- JSON com citações traduzidas
  tr_updated_at     TEXT                  -- timestamp ISO8601
);

CREATE TABLE entry_form (
  form_id     INTEGER PRIMARY KEY,
  entry_id    INTEGER REFERENCES entry(entry_id) ON DELETE CASCADE,
  form        TEXT NOT NULL,              -- abăcī, abăcōrum
  form_norm   TEXT NOT NULL,              -- normalizado (sem acentos)
  kind        TEXT NOT NULL               -- gen., dat., abl., etc.
);

CREATE TABLE sense (
  sense_id    INTEGER PRIMARY KEY,
  entry_id    INTEGER REFERENCES entry(entry_id) ON DELETE CASCADE,
  sense_no    TEXT,                       -- I, II, A, B, 1, 2
  definition  TEXT,
  level       INTEGER                     -- profundidade na hierarquia
);
```

**Uso**: Dicionário autoritativo Latin→EN (Perseus Digital Library) + traduções PT-BR.

---

### 3. `gaffiot.db` — Gaffiot Latin→French (leitura)

**Schema**: similar ao LS, mas sem traduções PT. Usado como referência cruzada.

---

### 4. `token_latim_german.sqlite` — Lat→Deu Morfológico

**Schema**:
```sql
CREATE TABLE VOC (
  id              INTEGER PRIMARY KEY,
  lemma           TEXT NOT NULL,
  grammar         TEXT,                   -- N 2 M, V 1 1 TRANS
  lemma_norm      TEXT GENERATED          -- normalizado
);

CREATE TABLE FORM (
  id              INTEGER PRIMARY KEY,
  voc_id          INTEGER REFERENCES VOC(id),
  form            TEXT NOT NULL,          -- abăcō, abăcīs
  form_norm       TEXT GENERATED,
  case_info       TEXT                    -- Nom/Gen/Dat/Acc/Abl + Sing/Plur
);
```

**Uso**: Validação morfológica (gates 1-4 do pipeline OCR).

---

### 5. `dicionarios_unificados.sqlite` — Agregador (OntoLex-Lemon)

**Schema RDF-like**:
```sql
CREATE TABLE dicionario (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  dicionario      TEXT,                   -- 'faria', 'ls', 'gaffiot'
  sujeito_uri     TEXT,                   -- URI do verbete
  tipo            TEXT,
  label           TEXT,                   -- lema legível
  campo           TEXT,                   -- ontolex#writtenRep, skos#definition
  valor           TEXT,
  outros_uris     TEXT
);
```

**Uso**: Agregação semântica de múltiplos dicionários. Permite queries tipo:
```sql
-- Todas as definições de "virtus" em todos os dicionários
SELECT dicionario, valor
FROM dicionario
WHERE label = 'virtus'
  AND campo = 'skos#definition';
```

---

## Workflow Proposto

### A. Ingestão/Correção (já implementado)

```
PDF (Faria)
  → OCR (ocr_end2end_pipeline_v4.py)
  → FAISS similarity + morphological gates
  → OpenAI Batch API (parsing → JSON)
  → consolidacao_parse_check_0.py (cross-reference LS/Gaffiot/Whitaker)
  → retificado_v2.db (INSERT)
```

### B. Tradução LS (já implementado)

```
ls_dict.db (Perseus XML)
  → openai_parse_mt.py (traduz definições EN→PT)
  → batchstart.py (OpenAI Batch API)
  → UPDATE ls_dict.db SET tr_trad_pt = ...
```

### C. **Exportação Unificada** (novo script proposto)

```python
# export_dicionarios.py
# Gera .md, .html, .json, .epub a partir dos SQLites

import sqlite3
from pathlib import Path
from datetime import datetime

def export_to_markdown(db_path: str, output_path: str, dict_name: str):
    """
    Exporta um dicionário SQLite para Markdown humanizado.

    Args:
        db_path: caminho do .db
        output_path: caminho do .md de saída
        dict_name: 'faria', 'ls', 'unified'
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(f"# Dicionário {dict_name.upper()}\n")
        f.write(f"*Exportado em {datetime.now().isoformat()}*\n\n")

        if dict_name == 'faria':
            query = """
                SELECT lemma, morph_render, definicao, notas, conf
                FROM entry
                ORDER BY lemma COLLATE NOCASE
            """
            for row in conn.execute(query):
                f.write(f"## {row['lemma']}\n")
                if row['morph_render']:
                    f.write(f"- **Morfologia:** {row['morph_render']}\n")
                f.write(f"- **Definição:** {row['definicao']}\n")
                if row['notas']:
                    f.write(f"- **Notas:** {row['notas']}\n")
                f.write(f"- **Confiança:** {row['conf']}\n\n")

        elif dict_name == 'ls':
            query = """
                SELECT lemma, pos, tr_gloss_pt, tr_trad_pt, tr_notas
                FROM entry
                WHERE tr_trad_pt != ''
                ORDER BY lemma_sort
            """
            for row in conn.execute(query):
                f.write(f"## {row['lemma']}\n")
                if row['pos']:
                    f.write(f"- **Classe:** {row['pos']}\n")
                if row['tr_gloss_pt']:
                    f.write(f"- **Glosas:** {row['tr_gloss_pt']}\n")
                f.write(f"- **Tradução:** {row['tr_trad_pt']}\n")
                if row['tr_notas']:
                    f.write(f"- **Notas:** {row['tr_notas']}\n")
                f.write("\n")

    conn.close()
    print(f"✅ Exportado {dict_name} → {output_path}")

# Uso:
# python export_dicionarios.py --db retificado_v2.db --format md --out dicionario_faria.md

### D. **Pipeline para front-end (Astro + Pagefind)**

Fluxo atual (implementado):
```
retificado_v2.db
  → scripts/export_normalized_from_retificado_v2.py
      ⇢ resultados/normalized_results_v2.json
  → scripts/export_shards_from_normalized.py
      ⇢ resultados/shards/<vol>/shard_*.ndjson + index.json
  → scripts/render_publication_from_shards.py
      ⇢ web/public/data/<vol>/{meta,dict,lookup} + volumes.json
  → web/tools/build_pagefind_from_shards.mjs
      ⇢ web/public/pagefind/
  → astro (web/) consome web/public/data + pagefind.js
```

Campos propagados: `morph_render`, `morph_extra`, `conf`, `needs_review`, `redirect_only`, `morph_out_of_vocab`, além de definição/notas/lemas.

Atalhos no Makefile:
- `make data-export-v2` → gera normalized_results_v2.json
- `make data-shards` → shards NDJSON
- `make data-render` → artefatos públicos
- `make search-index` → Pagefind
- `make data-all` → tudo acima em sequência
```

---

## Queries Úteis

### 1. Cross-reference entre dicionários

```sql
-- Comparar definições Faria vs LS para "virtus"
SELECT
  'Faria' AS fonte,
  definicao AS texto
FROM retificado_v2.entry
WHERE lemma = 'virtus'

UNION ALL

SELECT
  'LS' AS fonte,
  tr_trad_pt AS texto
FROM ls_dict.entry
WHERE lemma = 'virtus' AND tr_trad_pt != '';
```

### 2. Identificar entradas que precisam revisão

```sql
-- Faria: baixa confiança OCR
SELECT lemma, conf, definicao
FROM entry
WHERE conf = 'low' OR needs_review = 1
ORDER BY lemma;

-- LS: sem tradução PT
SELECT lemma, pos
FROM entry
WHERE tr_trad_pt = '' OR tr_trad_pt IS NULL
ORDER BY lemma_sort
LIMIT 100;
```

### 3. Full-Text Search (FTS5)

```sql
-- Criar índice FTS5 (uma vez):
CREATE VIRTUAL TABLE faria_fts USING fts5(
  lemma,
  definicao,
  notas,
  content=entry
);

-- Buscar por palavra-chave
SELECT lemma, snippet(faria_fts, 1, '<b>', '</b>', '...', 20) AS trecho
FROM faria_fts
WHERE faria_fts MATCH 'guerra AND romano'
ORDER BY rank;
```

---

## Roadmap

### Curto Prazo
- [x] Consolidar schemas existentes
- [ ] Script `export_dicionarios.py` (markdown, JSON, HTML)
- [ ] Índices FTS5 em todos os bancos
- [ ] View `dicionarios_unificados.verbetes_completos` (JOIN Faria + LS + Gaffiot)

### Médio Prazo
- [ ] API REST Flask/FastAPI (`GET /api/v1/lemma/virtus`)
- [ ] Interface web de consulta (busca morfológica + cross-reference)
- [ ] Exportação EPUB/Kindle
- [ ] Integração com Anki (flashcards automáticos)

### Longo Prazo
- [ ] Dump público (Zenodo/Archive.org) com licença aberta
- [ ] Plugin Obsidian/Logseq para consulta inline
- [ ] Correção colaborativa via web (wiki-style)

---

## Manutenção

### Backup
```bash
# Backup diário automático
sqlite3 retificado_v2.db ".backup /backup/retificado_$(date +%Y%m%d).db"
sqlite3 ls_dict.db ".backup /backup/ls_dict_$(date +%Y%m%d).db"
```

### Versionamento de Schema
```bash
# Após mudanças no schema:
sqlite3 retificado_v2.db ".schema" > docs/schema_retificado_v2.sql
git add docs/schema_retificado_v2.sql
git commit -m "schema: adicionar coluna etymology em entry"
```

### Validação de Integridade
```sql
-- Verificar foreign keys
PRAGMA foreign_keys = ON;
PRAGMA integrity_check;

-- Contar entradas órfãs (sem lemma)
SELECT COUNT(*) FROM entry WHERE lemma IS NULL OR lemma = '';
```

---

## Conclusão

Esta arquitetura permite:
1. ✅ Trabalhar sempre com dados estruturados (SQLite)
2. ✅ Exportar para qualquer formato sob demanda
3. ✅ Queries complexas (cross-reference, FTS, agregações)
4. ✅ Versionamento e reprodutibilidade
5. ✅ Escalabilidade (novos dicionários = novas tabelas)

**Próximo passo recomendado**: implementar `export_dicionarios.py` para substituir o `dicionario_completo.md` estático por exportações geradas dinamicamente.
