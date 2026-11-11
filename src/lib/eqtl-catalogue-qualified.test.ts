import { expect, it } from "vitest";
import { eqtlFineNative, eqtlObservation } from "../../test/eqtl-catalogue-fixture";
import { qualifiedCatalogue } from "./eqtl-catalogue-qualified";
import { mergeEqtlSupport } from "./eqtl-support";

it("qualifies exact native fine-mapped gene/allele joins and retains duplicate nominal row references once", async () => {
  const native = eqtlFineNative();
  native.fine_mapping!.result!.rows.push({ ...native.fine_mapping!.result!.rows[0] });
  const support = qualifiedCatalogue(native, await eqtlObservation(native), [0, 1], "saved");
  expect(support.size).toBe(1);
  expect([...support.values()][0].references).toMatchObject([{ ensembl_id: "ENSG00000134057", nominal_row_indexes: [0, 1],
    source_data_access_id: "saved", native_fine_mapping_row: { pip: 0.99 }, credible_set_file_sha256: "d".repeat(64), tissue_ontology_id: "CL:0000576" }]);
  expect([...support.values()][0].references).toHaveLength(1);
});

it.each([{ pip: 0.9499 }, { pip: 1.1 }, { pip: "1" }, { cs_min_r2: 0.2499 }, { cs_min_r2: null },
  { cs_size: 0 }, { cs_id: "" }, { gene_id: "ENSG00000134058" }, { gene_id: null }, { molecular_trait_id: "other" },
  { beta: 0.4 }, { pvalue: 0.01 }, { se: 0.2 }])("excludes an unqualified credible-set row %#", async change => {
  const native = eqtlFineNative(); Object.assign(native.fine_mapping!.result!.rows[0], change);
  expect(qualifiedCatalogue(native, await eqtlObservation(native), [0, 1], "saved").size).toBe(0);
});

it("does not infer support from nominal p-values, absent fine mapping or unresolved tissues", async () => {
  const native = eqtlFineNative(); const observation = await eqtlObservation(native);
  expect(qualifiedCatalogue(native, observation, [], "saved").size).toBe(0);
  delete native.fine_mapping;
  expect(qualifiedCatalogue(native, observation, [0, 1], "saved").size).toBe(0);
  const unknown = eqtlFineNative(); unknown.dataset.tissue_id = "unknown";
  expect(qualifiedCatalogue(unknown, observation, [0, 1], "saved").size).toBe(0);
});

it.each([
  { tissues: [], ids: [], study: "BLUEPRINT", group: "monocyte", expected: 1, basis: "distinct_observed_tissue_context" },
  { tissues: ["Monocyte"], ids: ["CL_0000576"], study: "BLUEPRINT", group: "monocyte", expected: 1, basis: "already_counted_tissue" },
  { tissues: ["Liver"], ids: ["UBERON:0002107"], study: "BLUEPRINT", group: "monocyte", expected: 2, basis: "distinct_observed_tissue_context" },
  { tissues: ["Liver"], ids: [null], study: "BLUEPRINT", group: "monocyte", expected: 1, basis: "cross_source_tissue_overlap_unresolved" },
  { tissues: ["Whole_Blood"], ids: ["UBERON:0013756"], study: "GTEx", group: "whole_blood", expected: 1, basis: "already_counted_tissue" },
])("merges tissue support using native identities and preserves unresolved overlaps %#", async c => {
  const native = eqtlFineNative(); const support = [...qualifiedCatalogue(native, await eqtlObservation(native), [0, 1], "saved").values()][0];
  support.study_label = c.study; support.sample_group = c.group;
  const merged = mergeEqtlSupport({ tissues: new Set(c.tissues), references: c.ids.map(tissue_ontology_id => ({ tissue_ontology_id })) }, support);
  expect(merged.tissues.size).toBe(c.expected);
  expect(merged.references[merged.references.length - 1]).toMatchObject({ tissue_count_basis: c.basis });
});
