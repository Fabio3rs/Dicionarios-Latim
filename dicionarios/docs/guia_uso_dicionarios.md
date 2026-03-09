# Guia de Uso — Sistema de Dicionários Latinos

## Visão Geral

Este projeto mantém **bancos SQLite como fonte da verdade** para dicionários latinos digitalizados. Exportações para `.md`, `.json`, `.html` são geradas sob demanda a partir dos bancos.

---

## 🗄️ Bancos Disponíveis

### 1. `retificado_v2.db` — Dicionário Faria
- **Origem**: Ernesto Faria, *Dicionário Latino-Português* (digitalizado via OCR)
- **Entradas**: 34.833
- **Campos principais**:
  - `lemma` — forma canônica (a, ab, abacus)
  - `morph_render` — morfologia normalizada (s. masc. 2ª, v. 1ª)
  - `definicao` — texto em português
  - `conf` — confiança da correção OCR (high/med/low)
  - `needs_review` — flag para revisão manual

### 2. `ls_dict.db` — Lewis & Short Traduzido
- **Origem**: Perseus Digital Library + traduções OpenAI
- **Entradas**: ~42.000 (nem todas traduzidas)
- **Campos principais**:
  - `lemma`, `pos`, `gen_text` — morfologia original (Perseus)
  - `tr_gloss_pt` — glosas curtas PT-BR
  - `tr_trad_pt` — tradução completa da definição
  - `tr_notas` — notas do tradutor

### 3. `gaffiot.db` — Gaffiot (referência)
- **Origem**: Félix Gaffiot, *Dictionnaire Latin-Français*
- **Uso**: cross-reference para validação de OCR

### 4. `token_latim_german.sqlite` — Lat→Deu Morfológico
- **Origem**: Latin-German morphological database
- **Uso**: validação morfológica (gates do pipeline OCR)

---

## 📤 Exportação de Dicionários

### Uso Básico

```bash
# Exportar Faria para Markdown
python export_dicionarios.py \
    --db retificado_v2.db \
    --format md \
    --out dicionario_faria.md

# Exportar LS para JSON (API-ready)
python export_dicionarios.py \
    --db ls_dict.db \
    --format json \
    --out ls_dict.json

# Exportar visão unificada (Faria + LS lado a lado)
python export_dicionarios.py \
    --unified \
    --format md \
    --out dicionario_completo.md

### Pipeline para busca web (Astro + Pagefind)

```bash
# 1) Exportar do SQLite oficial
make data-export-v2
# 2) Gerar shards NDJSON
make data-shards
# 3) Renderizar artefatos web/public/data
make data-render
# 4) Construir índice Pagefind
make search-index
# (ou tudo de uma vez)
make data-all
# Servir/compilar o front-end
cd web && npm run dev   # ou npm run build
```

Fonte oficial: `dicionarios/retificado_v2.db` → `resultados/normalized_results_v2.json` → shards → `web/public/data/**` → Pagefind `web/public/pagefind/`.
```

### Formatos Suportados

| Formato | Uso | Tamanho Típico |
|---------|-----|----------------|
| `md` | Leitura humana, documentação | 40-60 MB |
| `json` | APIs, processamento automático | 20-30 MB |
| `html` | Web, publicação online | 50-80 MB (com CSS) |
| `tsv` | Planilhas, análise estatística | 15-25 MB |

---

## 🔍 Queries Úteis

### 1. Buscar Lema Específico

```sql
-- Faria
sqlite3 retificado_v2.db
> SELECT lemma, morph_render, definicao, conf
  FROM entry
  WHERE lemma = 'virtus';

-- LS
sqlite3 ls_dict.db
> SELECT lemma, pos, tr_gloss_pt, tr_trad_pt
  FROM entry
  WHERE lemma = 'virtus' AND tr_trad_pt != '';
```

### 2. Cross-Reference Entre Dicionários

```sql
-- Attach múltiplos bancos
sqlite3 retificado_v2.db
> ATTACH 'ls_dict.db' AS ls;

> SELECT
    'Faria' AS fonte,
    f.lemma,
    f.definicao
  FROM entry f
  WHERE f.lemma = 'bellum'

  UNION ALL

  SELECT
    'LS' AS fonte,
    l.lemma,
    l.tr_trad_pt
  FROM ls.entry l
  WHERE l.lemma = 'bellum' AND l.tr_trad_pt != '';
```

### 3. Entradas que Precisam Revisão

```sql
-- Faria: baixa confiança OCR
SELECT lemma, conf, definicao
FROM entry
WHERE conf = 'low' OR needs_review = 1
ORDER BY lemma
LIMIT 50;

-- LS: sem tradução PT
SELECT lemma, pos, homograph_no
FROM entry
WHERE (tr_trad_pt = '' OR tr_trad_pt IS NULL)
  AND pos IN ('n.', 'v.', 'adj.')  -- priorizar substantivos/verbos/adjetivos
ORDER BY lemma_sort
LIMIT 100;
```

### 4. Busca Morfológica

```sql
-- Todos os verbos de 1ª conjugação
SELECT lemma, morph_render, definicao
FROM entry
WHERE morph_render LIKE '%v. 1ª%'
ORDER BY lemma;

-- Substantivos masculinos de 2ª declinação
SELECT lemma, morph_render, definicao
FROM entry
WHERE morph_render LIKE '%s. masc. 2ª%'
ORDER BY lemma;
```

### 5. Full-Text Search (FTS5)

```sql
-- Criar índice FTS5 (uma vez):
CREATE VIRTUAL TABLE faria_fts USING fts5(
  lemma,
  definicao,
  notas,
  content=entry,
  tokenize='unicode61 remove_diacritics 1'
);

-- Popular índice
INSERT INTO faria_fts(rowid, lemma, definicao, notas)
SELECT id, lemma, definicao, notas FROM entry;

-- Buscar por palavra-chave
SELECT
  lemma,
  snippet(faria_fts, 1, '<b>', '</b>', '...', 30) AS trecho
FROM faria_fts
WHERE faria_fts MATCH 'guerra romano'
ORDER BY rank
LIMIT 20;
```

---

## 🛠️ Manutenção

### Backup Automático

```bash
#!/bin/bash
# backup_dicionarios.sh

BACKUP_DIR="/backup/dicionarios"
DATE=$(date +%Y%m%d)

sqlite3 retificado_v2.db ".backup $BACKUP_DIR/retificado_v2_$DATE.db"
sqlite3 ls_dict.db ".backup $BACKUP_DIR/ls_dict_$DATE.db"

# Compactar backups antigos (> 7 dias)
find $BACKUP_DIR -name "*.db" -mtime +7 -exec gzip {} \;

# Deletar backups compactados > 90 dias
find $BACKUP_DIR -name "*.db.gz" -mtime +90 -delete

echo "✅ Backup completo: $(ls -lh $BACKUP_DIR/*_$DATE.db)"
```

### Verificação de Integridade

```bash
#!/bin/bash
# check_integrity.sh

for db in retificado_v2.db ls_dict.db gaffiot.db; do
    echo "Verificando $db..."
    sqlite3 $db "PRAGMA integrity_check;" | grep -q "ok"

    if [ $? -eq 0 ]; then
        echo "✅ $db: OK"
    else
        echo "❌ $db: CORROMPIDO!"
        exit 1
    fi
done

echo "✅ Todos os bancos íntegros"
```

### Versionamento de Schema

```bash
# Após mudanças no schema, salvar snapshot
sqlite3 retificado_v2.db ".schema" > docs/schemas/retificado_v2_$(date +%Y%m%d).sql

git add docs/schemas/
git commit -m "schema: adicionar coluna etymology em entry"
git tag -a schema-v1.2 -m "Schema v1.2: adicionar etimologia"
git push --tags
```

---

## 📊 Estatísticas

### Contar Entradas por Confiança (Faria)

```sql
SELECT
  conf,
  COUNT(*) AS total,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM entry), 1) AS percentual
FROM entry
GROUP BY conf
ORDER BY
  CASE conf
    WHEN 'high' THEN 1
    WHEN 'med' THEN 2
    WHEN 'low' THEN 3
  END;
```

### Contar Traduções LS por Classe Gramatical

```sql
SELECT
  pos,
  COUNT(*) AS total_entradas,
  SUM(CASE WHEN tr_trad_pt != '' THEN 1 ELSE 0 END) AS traduzidas,
  ROUND(100.0 * SUM(CASE WHEN tr_trad_pt != '' THEN 1 ELSE 0 END) / COUNT(*), 1) AS perc_traduzido
FROM entry
WHERE pos IS NOT NULL
GROUP BY pos
ORDER BY total_entradas DESC
LIMIT 20;
```

### Top 20 Lemas Mais Frequentes (análise de exemplos)

```sql
-- Assumindo uma tabela de exemplos/citações
SELECT
  lemma,
  COUNT(*) AS ocorrencias
FROM exemplos
GROUP BY lemma
ORDER BY ocorrencias DESC
LIMIT 20;
```

---

## 🚀 Casos de Uso Avançados

### 1. API REST com FastAPI

```python
# api_dicionarios.py
from fastapi import FastAPI, HTTPException
import sqlite3
from typing import Optional

app = FastAPI(title="API Dicionários Latinos")

def get_db():
    conn = sqlite3.connect("retificado_v2.db", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/api/v1/lemma/{lemma}")
def buscar_lemma(lemma: str, fonte: Optional[str] = "faria"):
    conn = get_db()

    if fonte == "faria":
        result = conn.execute(
            "SELECT * FROM entry WHERE lemma = ?",
            (lemma,)
        ).fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="Lema não encontrado")

    return dict(result)

# Executar: uvicorn api_dicionarios:app --reload
```

### 2. Plugin Obsidian/Logseq

```javascript
// obsidian-plugin-latim.js
// Busca inline de lemas em notas Markdown

async function buscarLemmaInline(lemma) {
    const response = await fetch(`http://localhost:8000/api/v1/lemma/${lemma}`);
    const data = await response.json();

    return `**${data.lemma}** (${data.morph_render}): ${data.definicao}`;
}

// Uso em nota: [[virtus::latim]] → expande para definição
```

### 3. Exportar para Anki (Flashcards)

```python
# export_anki.py
import sqlite3
import csv

conn = sqlite3.connect("retificado_v2.db")
cur = conn.cursor()

with open("anki_latim.csv", "w", encoding="utf-8", newline="") as f:
    writer = csv.writer(f, delimiter="\t")
    writer.writerow(["Lemma", "Morfologia", "Definição", "Tags"])

    for row in cur.execute("""
        SELECT lemma, morph_render, definicao, conf
        FROM entry
        WHERE conf = 'high'  -- apenas alta confiança
        ORDER BY RANDOM()
        LIMIT 1000
    """):
        tags = f"latim {row[3]}"
        writer.writerow([row[0], row[1], row[2], tags])

print("✅ Exportado 1000 flashcards → anki_latim.csv")
# Importar no Anki: Tools → Import → anki_latim.csv
```

---

## 🐛 Troubleshooting

### Erro: "database is locked"

```bash
# Verificar processos usando o banco
fuser retificado_v2.db

# Forçar checkpoint WAL
sqlite3 retificado_v2.db "PRAGMA wal_checkpoint(TRUNCATE);"
```

### Exportação MD muito grande

```bash
# Exportar apenas letra A
sqlite3 retificado_v2.db <<EOF
.mode markdown
.output faria_letra_a.md
SELECT lemma, morph_render, definicao
FROM entry
WHERE lemma GLOB 'A*' OR lemma GLOB 'a*'
ORDER BY lemma COLLATE NOCASE;
EOF
```

### Query lenta

```sql
-- Criar índices adicionais
CREATE INDEX IF NOT EXISTS idx_lemma_norm ON entry(lemma COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_morph ON entry(morph_render);
CREATE INDEX IF NOT EXISTS idx_conf ON entry(conf);

-- Analisar query plan
EXPLAIN QUERY PLAN
SELECT * FROM entry WHERE lemma = 'virtus';
```

---

## 📚 Recursos Adicionais

- **Schema completo**: `docs/schemas/`
- **Logs de processamento**: `*.log` (batch.log, end2end.log)
- **Scripts de pipeline**: `ocr_end2end_pipeline_v4.py`, `consolidacao_parse_check_0.py`
- **Documentação OntoLex**: https://www.w3.org/2016/05/ontolex/

---

## 🎯 Roadmap

- [ ] Interface web de consulta (React + FastAPI)
- [ ] Exportação EPUB/Kindle
- [ ] Suporte a GTK (gematria/isopsefia)
- [ ] Integração com Perseus Scaife Viewer
- [ ] Dump público no Archive.org (licença CC-BY-SA)
