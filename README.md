# Dicionários de Latim para Português

## Contextualização

Minha jornada com este projeto começou com o desafio de criar e utilizar dicionários latim-português de alta qualidade para aplicações digitais. A maioria das obras de referência disponíveis está em formato PDF, muitas vezes com layout de duas colunas, o que dificulta a extração de texto estruturado por meio de OCR convencional.

Também percebi uma lacuna em termos de **dicionários offline** latim-português que sejam completos e compatíveis com linguagens de programação, contendo:

* Entradas lexicais e seus sentidos;
* Atributos morfológicos essenciais;
* Disponibilidade local, sem dependência de serviços externos.

Projetos internacionais adotam modelos como OntoLex-Lemon, que externalizam dados morfológicos para bancos como o LiLa Lemma Bank, acessíveis por SPARQL. Apesar de corretos conceitualmente, esses formatos criam dependência de acesso online — algo que não torna complicada a integração com outros projetos de software.

---

## Introdução para todos os públicos

Você já viu um PDF antigo — por exemplo, um dicionário — e tentou copiar o texto?
Muitas vezes, o que se copia são palavras quebradas, acentos trocados e símbolos estranhos. Isso acontece porque a maioria dos PDFs antigos é apenas uma **imagem escaneada**, e para “transformar” essa imagem em texto usamos **OCR** (*Optical Character Recognition*, ou Reconhecimento Óptico de Caracteres).

### Como funciona o OCR tradicional

Ferramentas como Tesseract analisam a imagem, tentam reconhecer letra por letra e remontar as palavras. Isso funciona bem para textos limpos e modernos, mas falha em casos como:

* Layout em **duas colunas**.
* Fontes antigas e caracteres especiais.
* Textos em latim com diacríticos (macrons, breves, ligaduras).
* Abreviações e notações específicas de obras lexicográficas.

O resultado são muitos erros de leitura que atrapalham a pesquisa e o processamento automático.

### Onde entra a OpenAI API

A **OpenAI API** é um serviço na nuvem que recebe conteúdo (texto, imagens etc.) e retorna uma resposta processada por um modelo de IA.

No nosso caso:

1. Enviamos à API imagens das páginas do dicionário (extraídas do PDF).
2. Pedimos ao modelo para **ler** e **estruturar** cada verbete, entendendo contexto, convenções lexicográficas e padrões de abreviação.
3. O modelo devolve o texto **corrigido, segmentado e enriquecido**, preservando acentos e símbolos.

**Por que isso é melhor que OCR comum?**
Porque o modelo entende o *sentido*. Se o OCR tradicional lê “anno vebis conditae”, ele não sabe que deveria ser *anno urbis conditae*. A IA detecta e corrige automaticamente, consultando o contexto linguístico.

---

## Objetivo Principal

Criar um **dicionário offline de latim para português** que seja completo, robusto e autocontido, fornecendo:

* Dados extraídos e limpos de PDFs originais;
* Normalização morfológica e lexical;
* Consulta rápida com **Full-Text Search (FTS5)**;
* Total portabilidade e uso local.

---

## Desafios centrais

1. **Extração de dados de alta qualidade** — superar as limitações do OCR convencional em PDFs complexos.
2. **Desenvolvimento de um dicionário offline completo** — incluir todas as informações relevantes no mesmo banco de dados.
3. **Garantir rastreabilidade** — manter o texto bruto intacto antes da normalização, para permitir auditoria.

---

## 🔄 Fluxo de processamento

```
PDF original
   │
   ├── OCR fiel via LLM (openai_test.py, GPT-4.1)
   │        ↓
   │   ocr_results.db — transcrição literal por página, sem correção de verbetes,
   │   preservando acentos, indentação e colunas originais.
   │
   ├── Unificação e ingestão de referência L&S (sqlingest2.py)
   │        ↓
   │   ls_dict.sqlite — entrada do Lewis & Short no formato SQL para consultas rápidas
   │   e apoio à etapa seguinte.
   │
   ├── Análise e segmentação (analisefaria.py)
   │        ↓
   │   chunks_faria.json — verbetes agrupados em blocos coerentes (chunks),
   │   com verificação de cabeçalhos de página e marcação de possíveis problemas.
   │
   ├── Reescrita e normalização assistida por LLM (openai_parse_mt.py, GPT-5)
   │        ↓
   │   parse_check.db — cada chunk é enriquecido com dados do L&S e do Whitaker’s Words,
   │   e reescrito/estruturado em JSON validado, mas ainda em fase de checagem.
   │
   ├── Normalização e ingestão final (normaliza_parse_check.py / ingest_normalized.py)
            ↓
        lexicon.db — base consolidada com FTS5, incluindo lemas, morfologia, definições e exemplos.
```

> 💡 **Por que GPT-4.1 no OCR e GPT-5 na normalização?**
>
> * **OCR fiel (GPT-4.1):** nesta fase o mais importante é a **precisão literal**, captando exatamente o que está impresso. O GPT-4.1 se mostrou excelente para manter diacríticos, alinhamento e caracteres especiais sem “inventar” correções.
> * **Normalização e reescrita (GPT-5):** nesta fase entra a compreensão de contexto, reconstrução de lemas e definições, correção de erros e uniformização. O GPT-5 trouxe melhor raciocínio e consistência, reduzindo falsos positivos.
>
> Essa separação evita que a IA “contamine” o OCR com correções prematuras, preservando o texto bruto para conferência e aplicando melhorias apenas na etapa certa.

---

## 📜 Scripts principais

* **`openai_test.py`** — OCR fiel página a página com GPT-4.1, grava em `ocr_results.db`.
* **`sqlingest2.py`** — ingere Lewis & Short (Perseus XML) em `ls_dict.sqlite` para apoio léxico.
* **`analisefaria.py`** — valida cabeçalhos e segmenta em `chunks_faria.json`.
* **`openai_parse_mt.py`** — reescreve e normaliza chunks com GPT-5, usando `ls_dict.sqlite` e Whitaker’s Words; salva em `parse_check.db`.
* **`normaliza_parse_check.py`** — consolida e uniformiza dados, exportando `normalized_results.json`.
* **`ingest_normalized.py`** — ingere `normalized_results.json` no `lexicon.db` com FTS5.
* **`query_lexicon.py`** — consultas ao `lexicon.db` com filtros avançados (ID, FTS, página, doc, status etc.).

---

## 📂 Estrutura do projeto

* `dicionarios/` — artefatos de dados resultantes do processamento.
* `resultados/` — arquivos de saída de análises e SQL final para ingestão.
* `scripts/` — todos os scripts Python descritos acima.

---

## 🚀 Instalação e Configuração

### Pré-requisitos

* Python 3.8+ (testado com Python 3.12)
* Chave da API OpenAI (para OCR e processamento)
* Poppler (para pdf2image):
  * **Ubuntu/Debian:** `sudo apt-get install poppler-utils`
  * **macOS:** `brew install poppler`
  * **Windows:** Baixe o binário do Poppler e adicione ao PATH

### Instalação

1. **Clone o repositório:**
   ```bash
   git clone https://github.com/Fabio3rs/Dicionarios-Latim.git
   cd Dicionarios-Latim
   ```

2. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure a chave da API OpenAI:**
   ```bash
   export OPENAI_API_KEY="sua_chave_aqui"
   ```
   
   Ou crie um arquivo `.env` no diretório raiz:
   ```
   OPENAI_API_KEY=sua_chave_aqui
   ```

### Uso Básico

**Para consultar o lexicon existente:**
```bash
python scripts/query_lexicon.py --db resultados/lexicon.db --query "puella"
```

**Para processar um novo PDF (requer configuração avançada):**
```bash
# 1. OCR do PDF
python scripts/openai_test.py

# 2. Análise e segmentação
python scripts/analisefaria.py

# 3. Normalização
python scripts/openai_parse_mt.py

# 4. Consolidação final
python scripts/normaliza_parse_check.py

# 5. Criação do banco de dados
python scripts/ingest_normalized.py --json normalized_results.json --db lexicon.db --schema schema_normalizado.sql
```

**Para consultas avançadas:**
```bash
python scripts/query_lexicon.py --help
```

### Estrutura de Dados

O projeto gera diferentes formatos de saída:
* `ocr_results.db` — OCR bruto por página
* `chunks_faria.json` — verbetes segmentados
* `normalized_results.json` — dados normalizados
* `lexicon.db` — banco final com FTS5

---

## 📜 Licença Geral

Este projeto é licenciado sob a **GNU General Public License v3.0 (GPL-3.0)**.

**Por que GPL v3 e não CC-BY-SA?**

* O núcleo deste repositório é **software** — scripts e código que processam, reescrevem e normalizam dados.
* A GPL v3 garante **copyleft forte**, exigindo que trabalhos derivados mantenham a mesma liberdade.
* É compatível com conteúdos sob **CC BY-SA 4.0** (conversão one-way possível).
* Inclui proteções adicionais contra DRM, concessão de patentes e clareza de distribuição de código.

---

## ⚠️ Avisos de Licença e Atribuição

Este projeto incorpora recursos de terceiros, cada um com licença ou status específico:

* **Whitaker’s Words** — uso irrestrito (“for whatever purpose”), atribuição apreciada mas não obrigatória. — William A. Whitaker ([Fonte](https://github.com/mk270/whitakers-words))
* **Lewis & Short (Perseus XML)** — texto do Perseus Digital Library sob **CC BY-SA 4.0** ([Fonte](https://github.com/PerseusDL/lexica)).
* **The Latin Library** — conteúdos majoritariamente de **domínio público**, compilados de várias fontes ([Credits](https://www.thelatinlibrary.com/cred.html), [About](https://www.thelatinlibrary.com/about.html)).
* **CIRCSE / Latin-Portuguese-dictionaries** — **CC-BY-4.0** ([Fonte](https://github.com/CIRCSE/Latin-Portuguese-dictionaries)).
* **Dicionário Escolar Latim-Português (Ernesto Faria)** — **CC0 1.0 Universal** ([Fonte](https://archive.org/details/DicionarioEscolarLatinoPortuguesDoMecPorErnestoFaria1962)).

**Disclaimer:** Este repositório pode estar **incompleto** e em evolução. Algumas etapas podem exigir scripts ou dados não totalmente publicados.
