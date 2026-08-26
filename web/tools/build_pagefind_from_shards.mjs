#!/usr/bin/env node
/**
 * Gera índice Pagefind a partir dos artefatos em web/public/data.
 *
 * Uso:
 *   node web/tools/build_pagefind_from_shards.mjs --public web/public --base /dicionarios
 */
import fs from "node:fs";
import path from "node:path";

function parseArgs(argv) {
  const args = { public: "web/public", base: "/Dicionarios-Latim" };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--public" && argv[i + 1]) {
      args.public = argv[++i];
    } else if (a === "--base" && argv[i + 1]) {
      args.base = argv[++i];
    }
  }
  return args;
}

function readJSON(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function normalizeFirstLetter(s) {
  if (!s) return "#";
  return s.normalize("NFD").replace(/\p{M}+/gu, "").slice(0, 1).toUpperCase();
}

function normalizeLemma(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/\p{M}+/gu, "")
    .toLocaleLowerCase("la")
    .replace(/\s+/g, " ")
    .trim();
}

function compactText(parts) {
  return parts
    .flatMap((part) => (Array.isArray(part) ? part : [part]))
    .filter(Boolean)
    .join("\n");
}

function pageBucket(pageNum, bucket = 100) {
  return Math.floor((pageNum || 0) / bucket);
}

async function main() {
  const argv = parseArgs(process.argv);
  const pagefind = await import("pagefind");
  const publicDir = path.resolve(argv.public);
  const base = (argv.base || "").replace(/\/$/, "") || "";

  const dataDir = path.join(publicDir, "data");
  const volumesPath = path.join(dataDir, "volumes.json");
  const volumes = readJSON(volumesPath);

  const { index } = await pagefind.createIndex({
    rootSelector: "body",
  });

  if (!index) {
    throw new Error("O Pagefind não conseguiu criar o índice.");
  }

  for (const vol of volumes) {
    const volId = vol.volume_id ?? vol.id;
    const metaPath = path.join(publicDir, vol.meta_url);
    const meta = readJSON(metaPath);

    for (const blk of meta.blocks) {
      const blockPath = path.join(publicDir, vol.blocks_prefix, blk.file);
      const entries = readJSON(blockPath);

      for (const entry of entries) {
        const lemmas = [...new Set((entry.lemmas || []).filter(Boolean))];
        const firstLemma = lemmas[0] || "";
        // O lema precisa fazer parte do conteúdo, além dos metadados. Isso permite
        // que a busca literal do Pagefind encontre a entrada propriamente dita.
        const content = compactText([
          lemmas,
          entry.definicao,
          entry.translations,
          entry.exemplos,
          entry.notas,
        ]);
        const morph = entry.morph_render || entry.morfologia || "";
        const conf = entry.conf || "";
        const needsReview = entry.needs_review ? "1" : "0";

        const filters = {
          volume_id: [String(volId)],
          first_letter: [normalizeFirstLetter(firstLemma)],
          page_bucket: [String(pageBucket(entry.page_num))],
          // Filtro interno usado pela interface para colocar a entrada literal
          // antes dos resultados flexíveis produzidos pelo stemming.
          lemma_exact: lemmas.map(normalizeLemma).filter(Boolean),
        };
        if (entry.morfologia) {
          filters.morfologia = [entry.morfologia];
        }
        if (morph) {
          filters.morph_render = [morph];
        }
        if (conf) {
          filters.conf = [conf];
        }
        if (needsReview === "1") {
          filters.needs_review = ["1"];
        }

        await index.addCustomRecord({
          id: entry.id,
          url: `${base}/viewer?vol=${encodeURIComponent(volId)}&id=${encodeURIComponent(entry.id)}`,
          language: "pt",
          content,
          meta: {
            title: firstLemma,
            entry_id: String(entry.id),
            volume_id: String(volId),
            lemmas: lemmas.join(", "),
            morfologia: morph,
            page_num: String(entry.page_num ?? ""),
            first_letter: normalizeFirstLetter(firstLemma),
            conf,
            needs_review: needsReview,
            redirect_only: entry.redirect_only ? "1" : "0",
          },
          filters,
          sort: {
            lemma: normalizeLemma(firstLemma),
          },
        });
      }
    }
    console.log(`Indexed volume ${volId}`);
  }

  const outputPath = path.join(publicDir, "pagefind");
  // writeFiles não remove fragmentos de builds anteriores. Como cada verbete é
  // um registro, esses órfãos faziam o diretório crescer centenas de megabytes.
  fs.rmSync(outputPath, { recursive: true, force: true });
  await index.writeFiles({ outputPath });

  console.log(`Pagefind index escrito em ${outputPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
