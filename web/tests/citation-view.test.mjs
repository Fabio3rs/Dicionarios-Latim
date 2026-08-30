import assert from "node:assert/strict";
import test from "node:test";

import {cleanCitationPassage} from "../public/citation-view.mjs";

test("removes a leading locator missing its opening bracket and the next orphan bracket", () => {
  assert.equal(
    cleanCitationPassage("172] Verum accidit ut Carpinaltius.\n["),
    "Verum accidit ut Carpinaltius.",
  );
});

test("removes a complete leading bracketed locator", () => {
  assert.equal(
    cleanCitationPassage("[172] Verum accidit ut Carpinaltius."),
    "Verum accidit ut Carpinaltius.",
  );
});

test("preserves brackets that belong to the passage", () => {
  const passage = "Dareus tamen [proelio] victus est.";
  assert.equal(cleanCitationPassage(passage), passage);
});

test("leaves an ordinary passage unchanged", () => {
  assert.equal(cleanCitationPassage("Arma virumque cano."), "Arma virumque cano.");
});
