import type { L2gPredictionRow, OtStudy } from "./opentargets-contract";

/** Normalize CURIE punctuation only; never infer synonyms or ontology ancestry. */
export function traitId(value: unknown): string | null {
  if (typeof value !== "string" || !/^[A-Za-z][A-Za-z0-9]*[_:]\d+$/.test(value)) return null;
  return value.replace(":", "_");
}

export function studyMatchesTrait(study: OtStudy, selected: string | null): boolean {
  return selected !== null && study.studyType === "gwas" && Array.isArray(study.traitFromSourceMappedIds)
    && study.traitFromSourceMappedIds.some(id => traitId(id) === selected);
}

export function qualifyPredictionTrait(row: Pick<L2gPredictionRow, "study_json" | "credible_set_json">, requested: string | null) {
  const selected = traitId(requested);
  const result = { requested_trait_id: selected, trait_match: selected ? "unmatched" as const : "unresolved" as const };
  if (!selected) return result;
  try {
    const study = JSON.parse(row.study_json);
    const set = JSON.parse(row.credible_set_json);
    if (!study || typeof study.id !== "string" || !study.id || !set || set.studyId !== study.id
      || set.studyType !== "gwas" || !studyMatchesTrait(study, selected)) return result;
    return { requested_trait_id: selected, trait_match: "exact_mapped_trait_id" as const };
  } catch { return result; }
}

export function qualifyBundleTraits(rows: L2gPredictionRow[], selected: string | null, warnings: string[]) {
  let excluded = 0;
  for (const row of rows) {
    Object.assign(row, qualifyPredictionTrait(row, selected));
    if (row.trait_match !== "exact_mapped_trait_id") excluded++;
  }
  if (excluded) warnings.push(`OpenTargets: ${excluded} predictions retained as context only; exact mapped GWAS trait and consistent study identity were not established for ${selected ?? "an unresolved trait"}`);
}
