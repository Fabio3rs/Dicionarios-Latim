
-- SQLite schema for lexicon with degraded citations handling + FTS5
-- Generated: 2025-08-12

PRAGMA foreign_keys = ON;

BEGIN;

----------------------------------------------------------------------
-- Core entries
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS entry (
  id            INTEGER PRIMARY KEY,
  doc_name      TEXT NOT NULL,
  page_num      INTEGER,
  raw_text      TEXT,
  morfologia    TEXT,
  definicao     TEXT,
  notas         TEXT,
  created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS lemma (
  id        INTEGER PRIMARY KEY,
  entry_id  INTEGER NOT NULL REFERENCES entry(id) ON DELETE CASCADE,
  forma     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS variante (
  id        INTEGER PRIMARY KEY,
  lemma_id  INTEGER NOT NULL REFERENCES lemma(id) ON DELETE CASCADE,
  forma     TEXT NOT NULL,
  tipo      TEXT
);

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_entry_doc ON entry(doc_name, page_num);
CREATE INDEX IF NOT EXISTS idx_lemma_forma ON lemma(forma);
CREATE INDEX IF NOT EXISTS idx_variante_forma ON variante(forma);

----------------------------------------------------------------------
-- Raw + normalized citations (examples)
----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS exemplo_raw (
  id          INTEGER PRIMARY KEY,
  entry_id    INTEGER NOT NULL REFERENCES entry(id) ON DELETE CASCADE,
  raw_ref     TEXT NOT NULL,  -- raw reference string as imported
  texto       TEXT,           -- optional literal quotation
  origem      TEXT,           -- e.g., 'LS', 'OLD', 'Faria'
  imported_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS exemplo_norm (
  id            INTEGER PRIMARY KEY,
  raw_id        INTEGER NOT NULL UNIQUE REFERENCES exemplo_raw(id) ON DELETE CASCADE,
  parse_status  TEXT NOT NULL CHECK(parse_status IN ('ok','parcial','invalido')) DEFAULT 'invalido',
  err_msg       TEXT,
  fonte_abrev   TEXT,   -- e.g., 'Aul. Gell.', 'Apul.', '1 Pet.'
  obra_abrev    TEXT,   -- e.g., 'Apol.', 'Met.', 'Top.'
  loc_str       TEXT,   -- free-form location string: "3, 7, 6", "p. 284, 1", etc.
  autor_id      INTEGER,
  obra_id       INTEGER,
  livro         INTEGER,
  cap           INTEGER,
  secao         TEXT,
  pagina        INTEGER,
  linha         TEXT,
  confidence    REAL     -- 0..1
);

-- Optional lookup tables for canonical authors/works (can be populated later)
CREATE TABLE IF NOT EXISTS autor (
  id    INTEGER PRIMARY KEY,
  nome  TEXT NOT NULL,      -- canonical name, e.g., 'Aulus Gellius'
  abrev TEXT                -- preferred abbreviation
);

CREATE TABLE IF NOT EXISTS obra (
  id        INTEGER PRIMARY KEY,
  autor_id  INTEGER REFERENCES autor(id) ON DELETE SET NULL,
  titulo    TEXT NOT NULL,  -- canonical title, e.g., 'Noctes Atticae'
  abrev     TEXT            -- preferred abbreviation, e.g., 'NA'
);

CREATE INDEX IF NOT EXISTS idx_exemplo_raw_entry ON exemplo_raw(entry_id);
CREATE INDEX IF NOT EXISTS idx_exemplo_norm_status ON exemplo_norm(parse_status);
CREATE INDEX IF NOT EXISTS idx_exemplo_norm_abrevs ON exemplo_norm(fonte_abrev, obra_abrev);

----------------------------------------------------------------------
-- Views for clean consumption
----------------------------------------------------------------------

-- What the UI should read to display a reference string + status.
CREATE VIEW IF NOT EXISTS exemplo_view AS
SELECT
  r.id AS exemplo_id,
  r.entry_id,
  COALESCE(
    trim(
      COALESCE(n.fonte_abrev,'') || ' ' ||
      COALESCE(n.obra_abrev,'') || ' ' ||
      COALESCE(n.loc_str,'')
    ),
    r.raw_ref
  ) AS referencia,
  COALESCE(n.parse_status, 'invalido') AS status,
  r.texto,
  r.raw_ref,
  n.err_msg,
  n.confidence
FROM exemplo_raw r
LEFT JOIN exemplo_norm n ON n.raw_id = r.id;

----------------------------------------------------------------------
-- FTS5: entries
----------------------------------------------------------------------

-- Contentless FTS over flattened lemmas + entry fields
CREATE VIRTUAL TABLE IF NOT EXISTS entry_fts USING fts5(
  content='',
  content_rowid='rowid',
  lemas,
  definicao,
  morfologia,
  notas,
  raw_text,
  tokenize = 'unicode61 remove_diacritics 2'
);

-- Source projection to (re)populate the FTS
CREATE VIEW IF NOT EXISTS entry_fts_source AS
SELECT
  e.id AS rowid,
  (SELECT group_concat(forma, ' ') FROM lemma WHERE entry_id = e.id) AS lemas,
  e.definicao,
  e.morfologia,
  e.notas,
  e.raw_text
FROM entry e;

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS entry_ai AFTER INSERT ON entry BEGIN
  INSERT INTO entry_fts(rowid, lemas, definicao, morfologia, notas, raw_text)
  SELECT e.id,
         (SELECT group_concat(forma,' ') FROM lemma WHERE entry_id=e.id),
         e.definicao, e.morfologia, e.notas, e.raw_text
  FROM entry e WHERE e.id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS entry_au AFTER UPDATE ON entry BEGIN
  INSERT INTO entry_fts(entry_fts, rowid) VALUES('delete', NEW.id);
  INSERT INTO entry_fts(rowid, lemas, definicao, morfologia, notas, raw_text)
  SELECT e.id,
         (SELECT group_concat(forma,' ') FROM lemma WHERE entry_id=e.id),
         e.definicao, e.morfologia, e.notas, e.raw_text
  FROM entry e WHERE e.id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS entry_ad AFTER DELETE ON entry BEGIN
  INSERT INTO entry_fts(entry_fts, rowid) VALUES('delete', OLD.id);
END;

CREATE TRIGGER IF NOT EXISTS lemma_ai AFTER INSERT ON lemma BEGIN
  INSERT INTO entry_fts(entry_fts, rowid) VALUES('delete', NEW.entry_id);
  INSERT INTO entry_fts(rowid, lemas, definicao, morfologia, notas, raw_text)
  SELECT e.id,
         (SELECT group_concat(forma,' ') FROM lemma WHERE entry_id=e.id),
         e.definicao, e.morfologia, e.notas, e.raw_text
  FROM entry e WHERE e.id = NEW.entry_id;
END;

CREATE TRIGGER IF NOT EXISTS lemma_au AFTER UPDATE ON lemma BEGIN
  INSERT INTO entry_fts(entry_fts, rowid) VALUES('delete', NEW.entry_id);
  INSERT INTO entry_fts(rowid, lemas, definicao, morfologia, notas, raw_text)
  SELECT e.id,
         (SELECT group_concat(forma,' ') FROM lemma WHERE entry_id=e.id),
         e.definicao, e.morfologia, e.notas, e.raw_text
  FROM entry e WHERE e.id = NEW.entry_id;
END;

CREATE TRIGGER IF NOT EXISTS lemma_ad AFTER DELETE ON lemma BEGIN
  INSERT INTO entry_fts(entry_fts, rowid) VALUES('delete', OLD.entry_id);
  INSERT INTO entry_fts(rowid, lemas, definicao, morfologia, notas, raw_text)
  SELECT e.id,
         (SELECT group_concat(forma,' ') FROM lemma WHERE entry_id=e.id),
         e.definicao, e.morfologia, e.notas, e.raw_text
  FROM entry e WHERE e.id = OLD.entry_id;
END;

----------------------------------------------------------------------
-- FTS5: examples (uses exemplo_raw.id as rowid)
----------------------------------------------------------------------

CREATE VIRTUAL TABLE IF NOT EXISTS exemplo_fts USING fts5(
  content='',
  content_rowid='rowid',
  raw_ref,
  texto,
  fonte_abrev,
  obra_abrev,
  loc_str,
  tokenize='unicode61 remove_diacritics 2'
);

CREATE VIEW IF NOT EXISTS exemplo_fts_source AS
SELECT
  r.id AS rowid,
  r.raw_ref,
  r.texto,
  n.fonte_abrev,
  n.obra_abrev,
  n.loc_str
FROM exemplo_raw r
LEFT JOIN exemplo_norm n ON n.raw_id = r.id;

-- Helper trigger to (re)index one exemplo row by ID
CREATE TRIGGER IF NOT EXISTS exemplo_reindex AFTER INSERT ON exemplo_norm BEGIN
  INSERT INTO exemplo_fts(exemplo_fts, rowid) VALUES('delete', NEW.raw_id);
  INSERT INTO exemplo_fts(rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str)
  SELECT rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str
  FROM exemplo_fts_source WHERE rowid = NEW.raw_id;
END;

-- Keep FTS in sync for raw inserts
CREATE TRIGGER IF NOT EXISTS exemplo_raw_ai AFTER INSERT ON exemplo_raw BEGIN
  INSERT INTO exemplo_fts(rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str)
  SELECT rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str
  FROM exemplo_fts_source WHERE rowid = NEW.id;
END;

-- Updates on exemplo_raw
CREATE TRIGGER IF NOT EXISTS exemplo_raw_au AFTER UPDATE ON exemplo_raw BEGIN
  INSERT INTO exemplo_fts(exemplo_fts, rowid) VALUES('delete', NEW.id);
  INSERT INTO exemplo_fts(rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str)
  SELECT rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str
  FROM exemplo_fts_source WHERE rowid = NEW.id;
END;

-- Deletions
CREATE TRIGGER IF NOT EXISTS exemplo_raw_ad AFTER DELETE ON exemplo_raw BEGIN
  INSERT INTO exemplo_fts(exemplo_fts, rowid) VALUES('delete', OLD.id);
END;

-- Updates on exemplo_norm (status/fields change)
CREATE TRIGGER IF NOT EXISTS exemplo_norm_au AFTER UPDATE ON exemplo_norm BEGIN
  INSERT INTO exemplo_fts(exemplo_fts, rowid) VALUES('delete', NEW.raw_id);
  INSERT INTO exemplo_fts(rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str)
  SELECT rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str
  FROM exemplo_fts_source WHERE rowid = NEW.raw_id;
END;

CREATE TRIGGER IF NOT EXISTS exemplo_norm_ad AFTER DELETE ON exemplo_norm BEGIN
  INSERT INTO exemplo_fts(exemplo_fts, rowid) VALUES('delete', OLD.raw_id);
  INSERT INTO exemplo_fts(rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str)
  SELECT rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str
  FROM exemplo_fts_source WHERE rowid = OLD.raw_id;
END;

----------------------------------------------------------------------
-- Convenience: manual (re)build procedures (run after bulk loads)
----------------------------------------------------------------------

-- Populate entry_fts from source (idempotent if you clear first)
-- Usage:
--   DELETE FROM entry_fts;
--   INSERT INTO entry_fts(rowid, lemas, definicao, morfologia, notas, raw_text)
--   SELECT rowid, lemas, definicao, morfologia, notas, raw_text FROM entry_fts_source;

-- Populate exemplo_fts from source (idempotent if you clear first)
-- Usage:
--   DELETE FROM exemplo_fts;
--   INSERT INTO exemplo_fts(rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str)
--   SELECT rowid, raw_ref, texto, fonte_abrev, obra_abrev, loc_str FROM exemplo_fts_source;

COMMIT;
