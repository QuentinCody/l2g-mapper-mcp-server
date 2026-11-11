import { expect, it } from "vitest";
import { qualifyPredictionTrait, traitId } from "./opentargets-traits";
import { qualifiedL2gScores } from "./qualified-l2g";
import type { L2gPredictionRow } from "./opentargets-contract";

const study = { id: "S1", studyType: "gwas", traitFromSource: "Label is not an identity", traitFromSourceMappedIds: ["EFO_1", "EFO_2"] };
const set = { studyId: "S1", studyType: "gwas" };
const row: L2gPredictionRow = {
  locus_id: "L1", lead_rsid: "rs1", study_locus_id: "SL1", symbol: "GENE", ensembl_id: "ENSG1", score: 0.9,
  data_version_json: '{"year":"2026","month":"09"}', model_version: null, variant_id: "1_10_A_G",
  features_json: "[]", shap_base_value: null, study_json: JSON.stringify(study), credible_set_json: JSON.stringify(set),
  genome_assembly: "GRCh38", anchor_match: "exact_resolved_variant_id", requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id",
};

it("matches native mapped IDs without collapsing namespaces or using labels", () => {
  expect(qualifyPredictionTrait(row, "EFO:1")).toEqual({ requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id" });
  expect(qualifyPredictionTrait(row, "EFO_2").trait_match).toBe("exact_mapped_trait_id");
  expect(qualifyPredictionTrait(row, "MONDO_1").trait_match).toBe("unmatched");
  for (const input of [null, "diabetes", "EFO_1 OR EFO_2", "http://example.org/EFO_1"]) expect(traitId(input)).toBeNull();
});

it.each([
  { study_json: JSON.stringify({ ...study, traitFromSourceMappedIds: [] }) },
  { study_json: JSON.stringify({ ...study, traitFromSourceMappedIds: "EFO_1" }) },
  { study_json: JSON.stringify({ ...study, studyType: "eqtl" }) },
  { study_json: JSON.stringify({ ...study, id: "OTHER" }) },
  { study_json: "null" }, { study_json: "malformed" },
  { credible_set_json: JSON.stringify({ ...set, studyType: "pqtl" }) },
  { credible_set_json: "{}" },
])("excludes an unqualified native trait/study even when a stored marker claims a match %#", change => {
  expect(qualifiedL2gScores([{ ...row, ...change }])).toMatchObject({ excluded: 1, scores: new Map() });
});

it("requires markers and the selected trait from gather metadata, not just internally consistent rows", () => {
  expect(qualifiedL2gScores([row], "EFO_1").eligibleRows).toEqual([row]);
  expect(qualifiedL2gScores([row], "EFO_2").excluded).toBe(1);
  expect(qualifiedL2gScores([row], null).excluded).toBe(1);
  expect(qualifiedL2gScores([{ ...row, trait_match: "unresolved" }]).excluded).toBe(1);
  expect(() => qualifiedL2gScores([row, { ...row, requested_trait_id: "EFO_2" }])).toThrow("span selected traits");
});
