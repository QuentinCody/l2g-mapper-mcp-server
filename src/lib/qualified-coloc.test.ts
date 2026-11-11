import { expect, it } from "vitest";
import type { ColocRow, L2gPredictionRow } from "./opentargets-contract";
import { qualifiedColocScores } from "./qualified-coloc";
import { l2gGeneKey } from "./qualified-l2g";

function fixture() {
  const study = { id: "S1", studyType: "gwas", traitFromSourceMappedIds: ["EFO_1"] };
  const left = { studyLocusId: "SL1", studyId: "S1", studyType: "gwas", variant: { id: "1_10_A_G" } };
  const native = { colocalisationMethod: "COLOC_PIP", h4: 0.9, clpp: null, rightStudyType: "eqtl", otherStudyLocus: { studyId: "Q1", studyLocusId: "QL1", studyType: "eqtl", qtlGeneId: "ENSG1", isTransQtl: false, study: { id: "Q1", studyType: "eqtl", target: { id: "ENSG1" }, biosample: { biosampleId: "UBERON_1" } } } };
  const common = { locus_id: "L1", lead_rsid: "rs1", study_locus_id: "SL1", ensembl_id: "ENSG1", data_version_json: '{"year":"2026","month":"09"}', study_json: JSON.stringify(study), credible_set_json: JSON.stringify(left), genome_assembly: "GRCh38" as const, anchor_match: "exact_resolved_variant_id" as const, requested_trait_id: "EFO_1", trait_match: "exact_mapped_trait_id" as const };
  const prediction: L2gPredictionRow = { ...common, symbol: "GENE", score: 0.8, model_version: null, features_json: "null", shap_base_value: null, variant_id: "1_10_A_G" };
  const row: ColocRow = { ...common, method: native.colocalisationMethod, h4: 0.9, clpp: null, other_study_id: "Q1", lead_variant_id: "1_10_A_G", native_coloc_json: JSON.stringify(native) };
  return { row, prediction, native, left, study };
}

it("joins a native molQTL gene to the exact candidate, credible set, allele, trait and release", () => {
  const { row, prediction } = fixture(); const result = qualifiedColocScores([row], [prediction], "EFO:1");
  expect(result.excluded).toBe(0);
  expect(result.scores.get(l2gGeneKey(prediction))).toEqual({ h4: { method: "COLOC_PIP", metric: "h4", value: 0.9, study_locus_id: "SL1", other_study_locus_id: "QL1", other_study_id: "Q1", ensembl_id: "ENSG1", right_study_type: "eqtl", biosample_id: "UBERON_1", is_trans_qtl: false, requested_trait_id: "EFO_1", data_version_json: row.data_version_json, lead_variant_id: "1_10_A_G" }, clpp: null });
});

it.each([{ ensembl_id: "ENSG2" }, { lead_rsid: "rs2" }, { locus_id: "L2" }, { study_locus_id: "SL2" }, { variant_id: "1_10_A_T" }, { data_version_json: '{"year":"2026","month":"06"}' }, { requested_trait_id: "EFO_2" }, { score: -1 }])("does not transfer evidence to another or unqualified prediction %j", change => {
  const { row, prediction } = fixture();
  expect(qualifiedColocScores([row], [{ ...prediction, ...change }], "EFO_1")).toMatchObject({ scores: new Map(), excluded: 1 });
});

it.each(["gwas", "unknown", "pqtl"])("requires consistent molecular study types, not just a rightStudyType label (%s)", type => {
  const { row, prediction, native } = fixture(); native.rightStudyType = type; row.native_coloc_json = JSON.stringify(native);
  expect(qualifiedColocScores([row], [prediction], "EFO_1").excluded).toBe(1);
});

it("requires consistent native gene and study identities and a distinct credible-set pair", () => {
  for (const change of ["gene", "study", "missingGene", "missingSet", "sameSet"]) {
    const { row, prediction, native } = fixture();
    if (change === "gene") native.otherStudyLocus.study.target.id = "ENSG2";
    if (change === "study") native.otherStudyLocus.study.id = "Q2";
    if (change === "missingGene") native.otherStudyLocus.qtlGeneId = "";
    if (change === "missingSet") native.otherStudyLocus.studyLocusId = "";
    if (change === "sameSet") native.otherStudyLocus.studyLocusId = "SL1";
    row.native_coloc_json = JSON.stringify(native);
    expect(qualifiedColocScores([row], [prediction], "EFO_1").excluded).toBe(1);
  }
});

it("rechecks trait and left credible-set/allele evidence instead of trusting stored markers", () => {
  for (const change of ["trait", "set", "allele", "malformed"]) {
    const { row, prediction, left, study } = fixture();
    if (change === "trait") study.traitFromSourceMappedIds = ["EFO_2"];
    if (change === "set") left.studyLocusId = "SL2";
    if (change === "allele") left.variant.id = "1_10_A_T";
    row.study_json = JSON.stringify(study); row.credible_set_json = JSON.stringify(left);
    if (change === "malformed") row.native_coloc_json = "{invalid";
    expect(qualifiedColocScores([row], [prediction], "EFO_1").excluded).toBe(1);
  }
  const { row, prediction } = fixture();
  expect(qualifiedColocScores([row], [prediction], null).excluded).toBe(1);
});

it.each([Number.NaN, Number.POSITIVE_INFINITY, -0.1, 1.1])("rejects invalid metric values %s rather than clamping them", value => {
  const { row, prediction, native } = fixture(); row.h4 = value; native.h4 = value; row.native_coloc_json = JSON.stringify(native);
  expect(qualifiedColocScores([row], [prediction], "EFO_1").excluded).toBe(1);
});

it("keeps H4 and CLPP maxima separate and does not reinterpret an unknown method", () => {
  const { row, prediction, native } = fixture();
  const ecaviar = { ...row, method: "eCAVIAR", h4: null, clpp: 0.8, native_coloc_json: JSON.stringify({ ...native, colocalisationMethod: "eCAVIAR", h4: null, clpp: 0.8 }) };
  const unknown = { ...row, method: "newMethod", native_coloc_json: JSON.stringify({ ...native, colocalisationMethod: "newMethod" }) };
  const result = qualifiedColocScores([row, ecaviar, unknown], [prediction], "EFO_1");
  expect(result.excluded).toBe(1);
  expect(result.scores.get(l2gGeneKey(prediction))).toMatchObject({ h4: { value: 0.9, method: "COLOC_PIP" }, clpp: { value: 0.8, method: "eCAVIAR" } });
  expect(qualifiedColocScores([{ ...row, h4: 1 }], [prediction], "EFO_1").excluded).toBe(1);
});

it("selects a deterministic representative for tied maxima without counting duplicates", () => {
  const { row, prediction, native } = fixture();
  const other = { ...row, native_coloc_json: JSON.stringify({ ...native, otherStudyLocus: { ...native.otherStudyLocus, studyLocusId: "QL2" } }) };
  for (const rows of [[row, other, row], [other, row]]) expect(qualifiedColocScores(rows, [prediction], "EFO_1").scores.get(l2gGeneKey(prediction))?.h4?.other_study_locus_id).toBe("QL1");
});
