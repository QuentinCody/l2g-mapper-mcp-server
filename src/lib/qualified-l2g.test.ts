import { expect, it } from "vitest";
import type { L2gPredictionRow } from "./opentargets-contract";
import { l2gGeneKey, qualifiedL2gScores } from "./qualified-l2g";
import { crossLocusRanking } from "./cross-locus-ranking";

const prediction: L2gPredictionRow = {
  locus_id: "L1", lead_rsid: "rs1", symbol: "GENE", ensembl_id: "ENSG1", score: 0.8,
  study_locus_id: "SL1", variant_id: "1_10_A_G", data_version_json: JSON.stringify({ year: "2026", month: "09" }),
  model_version: null, genome_assembly: "GRCh38", anchor_match: "exact_resolved_variant_id",
  features_json: "[]", shap_base_value: null, requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id",
  study_json: '{"id":"S1","studyType":"gwas","traitFromSourceMappedIds":["EFO_1"]}', credible_set_json: '{"studyId":"S1","studyType":"gwas"}',
};

it("joins stable IDs and anchors rather than symbols and retains the representative maximum", () => {
  const rows = [prediction, { ...prediction, symbol: "OLD_SYMBOL", score: 0.9, study_locus_id: "SL2" }];
  const { scores } = qualifiedL2gScores(rows);
  expect(scores.get(l2gGeneKey(prediction))).toMatchObject({ score: 0.9, study_locus_id: "SL2", variant_id: "1_10_A_G", ensembl_id: "ENSG1", data_version_json: prediction.data_version_json, model_version: null });
  for (const change of [{ ensembl_id: "ENSG2" }, { lead_rsid: "rs2" }, { locus_id: "L2" }, { ensembl_id: null }]) {
    expect(scores.has(l2gGeneKey({ ...prediction, ...change }))).toBe(false);
  }
});

it.each([null, -1, 1.01, Number.NaN, Number.POSITIVE_INFINITY, "0.9"])("excludes malformed score %s", score => {
  const row = { ...prediction, score } as L2gPredictionRow;
  expect(qualifiedL2gScores([row])).toMatchObject({ scores: new Map(), excluded: 1 });
});

it.each(["ensembl_id", "lead_rsid", "locus_id", "study_locus_id", "variant_id", "data_version_json", "genome_assembly", "anchor_match"])("requires %s qualification", field => {
  expect(qualifiedL2gScores([{ ...prediction, [field]: null }])).toMatchObject({ scores: new Map(), excluded: 1 });
});

it("rejects a mixed release even when only one release supplies the highest score", () => {
  expect(() => qualifiedL2gScores([prediction, { ...prediction, score: 0.1, data_version_json: JSON.stringify({ year: "2026", month: "10" }) }])).toThrow("span data releases");
});

it("chooses tied maxima deterministically without counting them as independent studies", () => {
  const rows = [prediction, { ...prediction, study_locus_id: "SL0" }];
  const a = qualifiedL2gScores(rows); const b = qualifiedL2gScores([...rows].reverse());
  expect(a.scores).toEqual(b.scores);
  expect(a.scores.get(l2gGeneKey(prediction))?.study_locus_id).toBe("SL0");
});

it("ranks stable gene IDs across loci and counts each locus once", () => {
  const row = { ...prediction, score: { overall_score: 0.4 } };
  const result = crossLocusRanking([
    row, { ...row, symbol: "ALIAS", locus_id: "L2", score: { overall_score: 0.2 } },
    { ...row, lead_rsid: "rs2", score: { overall_score: 0.3 } },
    { ...row, ensembl_id: "ENSG2", locus_id: "L3" },
    { ...row, ensembl_id: null, locus_id: "L4" }, { ...row, ensembl_id: null, locus_id: "L5" },
  ]);
  expect(result).toHaveLength(4);
  expect(result[0]).toMatchObject({ ensembl_id: "ENSG1", supporting_loci: 2, max_score: 0.4 });
  expect(result[0].mean_score).toBeCloseTo(0.3);
  expect(result.find(g => g.ensembl_id === "ENSG2")?.supporting_loci).toBe(1);
  expect(result.filter(g => g.ensembl_id === null).every(g => g.supporting_loci === 1)).toBe(true);
});
