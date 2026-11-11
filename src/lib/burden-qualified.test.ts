import { expect, it } from "vitest";
import { qualifiedBurden } from "./burden-qualified";
import { burdenObservation, nativeBurden } from "../../test/burden-fixture";

it("uses exact gene/trait evidence, preserving native method, source and response references", async () => {
  const page = await burdenObservation();
  const result = await qualifiedBurden([page, page], "EFO_1", "saved");
  expect(result.warnings).toEqual([]);
  expect(result.support.get("ENSG1")).toMatchObject({ p_value: 2e-8, references: [{ source_data_access_id: "saved", source_table: "opentargets_evidence", source_response_sha256: page.response_sha256, source_row_index: 0, locus_specific: false, native_evidence: { statisticalMethod: "SKAT-O", diseaseFromSourceId: "source-phenotype" } }] });
  expect(result.support.get("ENSG1")?.references).toHaveLength(1);
});

it.each([
  { datasourceId: "gwas_credible_sets" }, { datatypeId: "somatic_mutation" }, { target: { id: "ENSG2" } },
  { disease: { id: "EFO_2" } }, { diseaseFromSourceMappedId: "EFO_2" }, { diseaseFromSourceMappedId: null },
  { id: "" }, { studyId: null }, { statisticalMethod: "" }, { qualityControls: ["flag"] }, { qualityControls: null },
  { warningMessage: "unreliable" }, { pValueMantissa: 0 }, { pValueMantissa: -2 }, { pValueMantissa: "2" },
  { pValueExponent: 0 }, { pValueExponent: -0.5 }, { pValueExponent: null }, { pValueExponent: -999 },
])("does not score unsupported, mismatched or invalid native evidence: %j", async patch => {
  const native = nativeBurden(); Object.assign(native.data.disease.evidences.rows[0], patch);
  const result = await qualifiedBurden([await burdenObservation(native)], "EFO_1", "saved");
  expect(result.support.size).toBe(0); expect(result.warnings.length).toBeGreaterThan(0);
});

it.each([
  { response_sha256: "changed" }, { source_url: "https://example.org" }, { error: "partial" }, { usable: false },
  { query: "query Other {}" }, { variables_json: '{"genes":["ENSG2"],"trait":"EFO_1"}' },
  { variables_json: '{"genes":["ENSG1"],"trait":"EFO_2"}' }, { variables_json: "null" },
  { data_version_json: '{"year":"2026","month":"10"}' },
])("does not score missing or inconsistent observation provenance: %j", async patch => {
  const result = await qualifiedBurden([{ ...await burdenObservation(), ...patch }], "EFO_1", "saved");
  expect(result.support.size).toBe(0);
});

it("rejects GraphQL errors, undiscovered source and mixed release evidence", async () => {
  for (const change of ["errors", "source", "release"]) {
    const native = nativeBurden();
    if (change === "errors") Object.assign(native, { errors: [{ message: "partial" }] });
    if (change === "source") native.data.associationDatasources = [];
    if (change === "release") native.data.meta.dataVersion.month = "10";
    const page = await burdenObservation(native);
    const observations = change === "release" ? [page, { ...await burdenObservation(), phase: "credible-set detail" }] : [page];
    expect((await qualifiedBurden(observations, "EFO_1", "saved")).support.size).toBe(0);
  }
});

it("preserves a partial page without claiming absence or combining conflicting evidence IDs", async () => {
  const native = nativeBurden(3);
  native.data.disease.evidences.count = 100;
  native.data.disease.evidences.rows[1].id = "e0";
  native.data.disease.evidences.rows[1].pValueExponent = -20;
  const result = await qualifiedBurden([await burdenObservation(native)], "EFO_1", "saved");
  expect(result.support.get("ENSG1")?.references.map(ref => ref.evidence_id)).toEqual(["e2"]);
  expect(result.support.get("ENSG1")?.references[0].coverage_complete).toBe(false);
  expect(result.support.get("ENSG1")?.p_value).toBe(2e-8);
});

it("does not infer a requested trait from evidence or gene symbols", async () => {
  const page = await burdenObservation();
  for (const trait of [null, "asthma", "EFO_2"]) expect((await qualifiedBurden([page], trait, "saved")).support.size).toBe(0);
});

it.each([{ count: -1 }, { count: "1" }, { cursor: { unknown: true } }])("excludes malformed evidence page metadata: %j", async patch => {
  const native = nativeBurden(); Object.assign(native.data.disease.evidences, patch);
  expect((await qualifiedBurden([await burdenObservation(native)], "EFO_1", "saved")).support.size).toBe(0);
});
