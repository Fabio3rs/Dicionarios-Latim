/**
 * Removes locator fragments accidentally included in a citation passage.
 *
 * Some generated corpus records start at the digits inside `[172]` and end at
 * the opening bracket of the following locator. Keep this normalization in the
 * presentation layer so the immutable published source record remains intact.
 */
export function cleanCitationPassage(value) {
  return String(value ?? "")
    .replace(/^\s*(?:\[\s*)?\d+[a-z]?\s*\]\s*/iu, "")
    .replace(/\s*\[\s*$/u, "")
    .trim();
}
