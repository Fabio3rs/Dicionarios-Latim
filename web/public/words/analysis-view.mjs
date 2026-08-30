const morphologyLabels = {
  case: {
    nominative: "nominativo",
    genitive: "genitivo",
    dative: "dativo",
    accusative: "acusativo",
    ablative: "ablativo",
    vocative: "vocativo",
    locative: "locativo",
  },
  number: {singular: "singular", plural: "plural"},
  gender: {
    masculine: "masculino",
    feminine: "feminino",
    neuter: "neutro",
    common: "comum",
  },
  tense: {
    present: "presente",
    imperfect: "imperfeito",
    future: "futuro",
    perfect: "perfeito",
    pluperfect: "mais-que-perfeito",
    "future-perfect": "futuro perfeito",
  },
  voice: {active: "voz ativa", passive: "voz passiva"},
  mood: {
    indicative: "indicativo",
    subjunctive: "subjuntivo",
    imperative: "imperativo",
    infinitive: "infinitivo",
    participle: "particípio",
  },
  degree: {
    positive: "grau positivo",
    comparative: "comparativo",
    superlative: "superlativo",
  },
};

function translated(group, value) {
  return morphologyLabels[group]?.[value] ||
    String(value || "").replaceAll("-", " ");
}

export function morphologyDescription(morphology) {
  const parts = [];
  if (morphology.declension) parts.push(`${morphology.declension}ª declinação`);
  if (morphology.conjugation) parts.push(`${morphology.conjugation}ª conjugação`);
  for (const key of ["case", "number", "gender", "tense", "voice", "mood", "degree"]) {
    if (morphology[key]) parts.push(translated(key, morphology[key]));
  }
  if (morphology.person) parts.push(`${morphology.person}ª pessoa`);
  return [...new Set(parts)].join(" · ") || "forma reconhecida";
}

export function derivationDescription(derivation) {
  return (derivation?.steps || []).map((step) => {
    if (step.kind === "rewrite") {
      if (step.before && step.after) return `grafia ${step.before} → ${step.after}`;
      return step.type === "syncope" ? "síncope" : "variante ortográfica";
    }
    if (step.type === "tackon") return `enclítico -${step.text}`;
    if (step.type === "prefix") return `prefixo ${step.text}-`;
    if (step.type === "suffix") return `sufixo -${step.text}`;
    if (step.type === "tickon") return `elemento inicial ${step.text}-`;
    if (step.type === "packon") return `elemento composto -${step.text}`;
    return `${step.type} ${step.text}`;
  }).join(" · ");
}

export function analysisItems(document) {
  const items = [...(document?.hits || [])];
  for (const suggestion of document?.suggestions || []) {
    for (const segment of suggestion.segments || []) {
      items.push(...(segment.hits || []));
    }
  }
  return items;
}

export function decompositionDescription(hit) {
  const recognized = String(hit?.form?.recognized || "").trim();
  if (!recognized) return "";

  const before = [];
  const after = [];
  for (const step of hit?.derivation?.steps || []) {
    if (step.kind !== "addon" || !step.text) continue;
    if (step.type === "prefix" || step.type === "tickon") {
      before.push(`${step.text}-`);
    } else {
      after.push(`-${step.text}`);
    }
  }
  return [...before, recognized, ...after].join(" + ");
}

export function suggestionDescriptions(document) {
  return (document?.suggestions || []).flatMap((suggestion) => {
    const parts = (suggestion.segments || []).map((segment) => {
      const decompositions = [...new Set(
        (segment.hits || []).map(decompositionDescription).filter(Boolean),
      )];
      return decompositions.find((value) => value.includes(" + ")) ||
        decompositions[0] || String(segment.text || "").trim();
    }).filter(Boolean);
    if (parts.length === 0) return [];
    return [{method: suggestion.method, description: parts.join(" + ")}];
  });
}

function normalizeLemmaKey(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/\p{M}+/gu, "")
    .toLocaleLowerCase("la")
    .replace(/\s+/g, " ")
    .trim();
}

export function buildAnalysisView(document) {
  const items = analysisItems(document);
  const suggestions = suggestionDescriptions(document);
  const lemmaKeys = new Set();
  const lemmas = [];
  const grouped = new Map();

  for (const hit of items) {
    if (hit.kind === "lexical" || hit.kind === "compound") {
      const key = normalizeLemmaKey(hit.lemma);
      if (key && !lemmaKeys.has(key)) {
        lemmaKeys.add(key);
        lemmas.push({key, label: hit.lemma});
      }
    }

    const lemma = hit.kind === "artificial"
      ? `Número romano ${hit.artificial.value}`
      : hit.lemma;
    const groupKey = hit.kind === "artificial"
      ? `artificial:${hit.artificial.value}`
      : `${normalizeLemmaKey(lemma)}:${hit.partOfSpeech}`;
    if (!grouped.has(groupKey)) {
      grouped.set(groupKey, {
        lemma,
        partOfSpeech: hit.partOfSpeech,
        decompositions: [],
        forms: [],
      });
    }

    const parts = [];
    if (hit.form?.recognized) parts.push(hit.form.recognized);
    parts.push(morphologyDescription(hit.morphology || {}));
    const derivation = derivationDescription(hit.derivation);
    if (derivation) parts.push(derivation);
    const description = parts.join(" · ");
    const group = grouped.get(groupKey);
    const decomposition = decompositionDescription(hit);
    if (decomposition && !group.decompositions.includes(decomposition)) {
      group.decompositions.push(decomposition);
    }
    if (!group.forms.includes(description)) group.forms.push(description);
  }

  return {
    itemsCount: items.length,
    suggestions,
    lemmas,
    groups: [...grouped.values()],
  };
}
