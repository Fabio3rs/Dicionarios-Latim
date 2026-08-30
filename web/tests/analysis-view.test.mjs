import assert from "node:assert/strict";
import test from "node:test";

import {
  buildAnalysisView,
  decompositionDescription,
  derivationDescription,
  grammaticalLabel,
  morphologyDescription,
  suggestionDescriptions,
} from "../public/words/analysis-view.mjs";

function form(recognized) {
  return {stem: recognized, stemKey: null, ending: "", recognized};
}

function numeralMorphology() {
  return {
    kind: "numeral",
    declension: 2,
    variant: 0,
    case: null,
    number: null,
    gender: null,
    numeralType: "cardinal",
  };
}

test("shows the recognized base and enclitic without turning it into two words", () => {
  const view = buildAnalysisView({
    hits: [{
      kind: "lexical",
      lexemeId: 42,
      lemma: "studium",
      partOfSpeech: "noun",
      form: form("studiis"),
      morphology: {
        kind: "noun",
        declension: 2,
        variant: 1,
        case: "dative",
        number: "plural",
        gender: "neuter",
      },
      derivation: {
        method: "derived",
        steps: [{kind: "addon", id: 314, type: "tackon", text: "que"}],
      },
    }],
    suggestions: [],
  });

  assert.deepEqual(view.lemmas, [{key: "studium", label: "studium"}]);
  assert.deepEqual(view.groups[0].decompositions, ["studiis + -que"]);
  assert.match(view.groups[0].forms[0], /^studiis · /);
  assert.match(view.groups[0].forms[0], /enclítico -que$/);
});

test("keeps artificial readings separate from lexical lemmas", () => {
  const view = buildAnalysisView({
    hits: [{
      kind: "artificial",
      partOfSpeech: "numeral",
      form: form("IV"),
      morphology: numeralMorphology(),
      derivation: {method: "roman-numeral", steps: []},
      artificial: {method: "roman-numeral", value: 4, wellFormed: true},
    }],
    suggestions: [],
  });

  assert.deepEqual(view.lemmas, []);
  assert.equal(view.groups[0].lemma, "Número romano 4");
  assert.match(view.groups[0].forms[0], /^IV · /);
});

test("preserves addon and rewrite order in the presentation", () => {
  const view = buildAnalysisView({
    hits: [{
      kind: "lexical",
      lexemeId: 43,
      lemma: "praetor",
      partOfSpeech: "noun",
      form: form("praetoribus"),
      morphology: {kind: "noun"},
      derivation: {
        method: "orthographic",
        steps: [
          {kind: "addon", id: 314, type: "tackon", text: "que"},
          {
            kind: "rewrite",
            id: 1,
            type: "orthographic",
            rule: "pre-prae",
            before: "pre",
            after: "prae",
          },
        ],
      },
    }],
    suggestions: [],
  });

  assert.match(
    view.groups[0].forms[0],
    /enclítico -que · grafia pre → prae$/,
  );
});

test("groups visually equivalent lexemes while preserving distinct forms", () => {
  const baseHit = {
    kind: "lexical",
    lemma: "cuiusque",
    partOfSpeech: "pronoun",
    form: form("cuius"),
    derivation: {
      method: "derived",
      steps: [{kind: "addon", id: 314, type: "tackon", text: "que"}],
    },
  };
  const view = buildAnalysisView({
    hits: [
      {
        ...baseHit,
        lexemeId: 1,
        morphology: {kind: "pronoun", case: "genitive", number: "singular"},
      },
      {
        ...baseHit,
        lexemeId: 2,
        morphology: {kind: "pronoun", case: "genitive", number: "singular"},
      },
      {
        ...baseHit,
        lexemeId: 3,
        morphology: {kind: "pronoun", case: "dative", number: "singular"},
      },
    ],
    suggestions: [],
  });

  assert.equal(view.itemsCount, 3);
  assert.equal(view.groups.length, 1);
  assert.equal(view.groups[0].forms.length, 2);
});

test("shows packons as a separated final element", () => {
  assert.equal(
    derivationDescription({
      method: "derived",
      steps: [{kind: "addon", id: 334, type: "packon", text: "que"}],
    }),
    "elemento composto -que",
  );
  assert.equal(
    decompositionDescription({
      form: form("cuius"),
      derivation: {
        steps: [{kind: "addon", id: 334, type: "packon", text: "que"}],
      },
    }),
    "cuius + -que",
  );
});

test("presents a two-word suggestion with an enclitic on its final segment", () => {
  const document = {
    hits: [],
    suggestions: [{
      method: "two-words",
      splitAt: 3,
      classification: "unconstrained",
      segments: [
        {
          text: "res",
          hits: [{
            kind: "lexical",
            lexemeId: 1,
            lemma: "res",
            partOfSpeech: "noun",
            form: form("res"),
            morphology: {kind: "noun", case: "nominative", number: "singular"},
            derivation: {method: "regular", steps: []},
          }],
        },
        {
          text: "publicaque",
          hits: [{
            kind: "lexical",
            lexemeId: 2,
            lemma: "publicus",
            partOfSpeech: "adjective",
            form: form("publica"),
            morphology: {kind: "adjective", case: "nominative", number: "singular"},
            derivation: {
              method: "derived",
              steps: [{kind: "addon", id: 314, type: "tackon", text: "que"}],
            },
          }],
        },
      ],
    }],
  };

  assert.deepEqual(suggestionDescriptions(document), [{
    method: "two-words",
    description: "res + publica + -que",
  }]);
  const view = buildAnalysisView(document);
  assert.equal(view.itemsCount, 2);
  assert.equal(view.groups.length, 2);
  assert.equal(view.suggestions[0].description, "res + publica + -que");
});

test("normalizes quantities in lemma keys used by Pagefind", () => {
  const view = buildAnalysisView({
    hits: [{
      kind: "lexical",
      lexemeId: 44,
      lemma: "mālum",
      partOfSpeech: "noun",
      form: form("mālum"),
      morphology: {kind: "noun"},
      derivation: {method: "regular", steps: []},
    }],
    suggestions: [],
  });

  assert.deepEqual(view.lemmas, [{key: "malum", label: "mālum"}]);
});

test("presents deponent forms as passive morphology with active meaning", () => {
  for (const [recognized, person] of [["reor", 1], ["reris", 2]]) {
    const view = buildAnalysisView({
      hits: [{
        kind: "lexical",
        lexemeId: 32908,
        lemma: "reor",
        partOfSpeech: "verb",
        lexical: {
          partOfSpeech: "verb",
          conjugation: 2,
          variant: 1,
          verbKind: "deponent",
        },
        form: form(recognized),
        morphology: {
          kind: "verb",
          conjugation: 2,
          variant: 1,
          tense: "present",
          voice: "passive",
          mood: "indicative",
          person,
          number: "singular",
        },
        derivation: {method: "regular", steps: []},
      }],
      suggestions: [],
    });

    assert.equal(view.groups.length, 1);
    assert.equal(view.groups[0].verbKind, "deponent");
    assert.equal(view.groups[0].grammaticalLabel, "verbo depoente");
    assert.match(view.groups[0].forms[0], /forma passiva · sentido ativo/);
    assert.doesNotMatch(view.groups[0].forms[0], /voz passiva/);
    assert.match(view.groups[0].forms[0], new RegExp(`${person}ª pessoa$`));
  }
});

test("keeps ordinary active and passive verb forms distinct", () => {
  const common = {
    kind: "verb",
    conjugation: 1,
    tense: "present",
    mood: "indicative",
  };
  const active = morphologyDescription({...common, voice: "active"});
  const passive = morphologyDescription({...common, voice: "passive"});

  assert.match(active, /voz ativa/);
  assert.doesNotMatch(active, /sentido ativo/);
  assert.match(passive, /voz passiva/);
  assert.doesNotMatch(passive, /sentido ativo/);
});

test("keeps deponent and ordinary readings in separate presentation groups", () => {
  const baseHit = {
    kind: "lexical",
    lemma: "forma",
    partOfSpeech: "verb",
    form: form("forma"),
    morphology: {kind: "verb", voice: "passive"},
    derivation: {method: "regular", steps: []},
  };
  const view = buildAnalysisView({
    hits: [
      {
        ...baseHit,
        lexemeId: 1,
        lexical: {partOfSpeech: "verb", verbKind: "deponent"},
      },
      {
        ...baseHit,
        lexemeId: 2,
        lexical: {partOfSpeech: "verb", verbKind: "transitive"},
      },
    ],
    suggestions: [],
  });

  assert.deepEqual(
    view.groups.map(({grammaticalLabel: label}) => label),
    ["verbo depoente", "verbo"],
  );
  assert.equal(grammaticalLabel("verb", "semideponent"), "verbo semidepoente");
});
