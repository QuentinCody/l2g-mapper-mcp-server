import { expect, it } from "vitest";
import type { L2gPredictionRow } from "./opentargets-contract";
import { selectCandidateGenes } from "./candidate-genes";

const anchors = [{ rsid: "rs1", locus_id: "L1" }, { rsid: "rs2", locus_id: "L1" }];
function prediction(ensembl_id: string | null, score: number, lead_rsid = "rs1", symbol = "SHARED"): L2gPredictionRow {
  return { locus_id: "L1", lead_rsid, symbol, ensembl_id, score, study_locus_id: "SL1", variant_id: "1_10_A_G", genome_assembly: "GRCh38", anchor_match: "exact_resolved_variant_id", data_version_json: '{"year":"2026","month":"09"}', model_version: null, features_json: "[]", shap_base_value: null, requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id", study_json: '{"id":"S1","studyType":"gwas","traitFromSourceMappedIds":["EFO_1"]}', credible_set_json: '{"studyId":"S1","studyType":"gwas"}' };
}

it("selects the highest-scoring unique genes, not the first input symbols", () => {
  const warnings: string[] = [];
  const result = selectCandidateGenes([prediction("ENSG1", 0.2), prediction("ENSG2", 0.9, "rs2"), prediction("ENSG2", 0.7, "rs2", "ALIAS")], anchors, 1, warnings);
  expect(result.candidates).toEqual([{ locus_id: "L1", lead_rsid: "rs2", symbol: "ALIAS", ensembl_id: "ENSG2", symbols_json: '["ALIAS","SHARED"]', selection_rank: 1, selection_score: 0.9 }]);
  expect(result.selection).toMatchObject([{ eligible_genes: 2, selected_genes: 1, omitted_genes: 1, selection_basis: "maximum_qualified_l2g_score" }]);
  expect(warnings.join(" ")).toContain("retained 1 of 2");
});

it("preserves distinct same-symbol genes and every actual supporting anchor", () => {
  const rows = [prediction("ENSG1", 0.8), prediction("ENSG2", 0.7, "rs2"), prediction("ENSG1", 0.9, "RS2")];
  const result = selectCandidateGenes(rows, anchors, 2, []);
  expect(result.candidates.map(row => [row.ensembl_id, row.lead_rsid])).toEqual([["ENSG1", "rs1"], ["ENSG1", "rs2"], ["ENSG2", "rs2"]]);
  expect(result.selection[0].selected_genes).toBe(2);
  expect(selectCandidateGenes([...rows].reverse(), [...anchors].reverse(), 2, [])).toEqual(result);
});

it("breaks score ties deterministically by stable ID and keeps caps independent per locus", () => {
  const rows = [prediction("ENSG2", 0.9), prediction("ENSG1", 0.9), { ...prediction("ENSG3", 0.1, "rs3"), locus_id: "L2" }];
  const result = selectCandidateGenes(rows, [...anchors, { rsid: "rs3", locus_id: "L2" }], 1, []);
  expect(result.candidates.map(row => row.ensembl_id)).toEqual(["ENSG1", "ENSG3"]);
  expect(result.selection).toMatchObject([{ locus_id: "L1", omitted_genes: 1 }, { locus_id: "L2", omitted_genes: 0 }]);
});

it("does not reassign unknown anchors or unresolved gene identities to the first anchor", () => {
  const warnings: string[] = [];
  const result = selectCandidateGenes([prediction(null, 1), prediction("ENSG1", 1, "rs3"), { ...prediction("ENSG2", 1), locus_id: "L2" }, prediction("ENSG4", Number.NaN)], anchors, 2, warnings);
  expect(result.candidates).toEqual([]);
  expect(warnings.join(" ")).toContain("2 unqualified");
  expect(warnings.join(" ")).toContain("2 predictions without the exact");
});

it.each([0, -1, 11, 1.5, Number.NaN])("rejects invalid candidate caps %s", limit => {
  expect(() => selectCandidateGenes([], anchors, limit, [])).toThrow("max_genes_per_locus");
});
