import {expect, it} from "vitest";
import {finnGenVariantIdentity, joinFinnGenPhenotype} from "./finngen-join";

const variant = {chr: 10, pos: 112998590, ref: "C", alt: "T", varid: "10:112998590:C:T"};
const requested = "10:112998590-C-T";
const definition = {phenocode: "T2D", phenostring: "Synthetic selected phenotype", num_cases: 10};
const association = {phenocode: "T2D", phenostring: definition.phenostring, pval: 1e-100, beta: -0.25, n_case: 10, unknown: [null, "native"]};

it("matches exact native coordinates/alleles with the provider's numeric X encoding", () => {
  expect(finnGenVariantIdentity({variant}, requested).verified).toBe(true);
  expect(finnGenVariantIdentity({variant: {chr: 23, pos: 10, ref: "A", alt: "G", varid: "23:10:A:G"}}, "X:10-A-G").verified).toBe(true);
});
it.each([
  {chr: 11}, {pos: "112998590"}, {pos: 112998591}, {ref: "T", alt: "C"}, {alt: "A"},
  {chr: null}, {chrom: 11}, {varid: "10:112998590:T:C"}, {varid: null}, {ref: "c"},
])("refuses mismatched or ambiguous native identity: %j", extra => {
  expect(finnGenVariantIdentity({variant: {...variant, ...extra}}, requested).verified).toBe(false);
});
it.each([null, {}, {variant: {}}, {qc_variant_results: {}}])("does not infer identity from a requested URL or missing/QC response: %j", native => {
  expect(finnGenVariantIdentity(native, requested).verified).toBe(false);
});
it("joins one exact phenocode with native statistics and retains every native field", () => {
  const joined = joinFinnGenPhenotype({results: [{...association, phenocode: "OTHER"}, association]}, true, "T2D", definition);
  expect(joined).toMatchObject({joined: true, association_row_indexes: [1], association, trait_equivalence_verified: false, score_contribution: 0});
});
it.each([
  [false, definition, [association], "variant_identity_unverified"],
  [true, null, [association], "definition_unavailable"],
  [true, {...definition, phenocode: "OTHER"}, [association], "definition_phenocode_mismatch"],
  [true, definition, [{...association, phenocode: "t2d"}], "phenocode_not_returned"],
  [true, definition, [association, association], "ambiguous_phenocode"],
  [true, definition, [{...association, phenostring: "Different"}], "phenotype_label_mismatch"],
  [true, definition, [{...association, pval: -1}], "invalid_association_statistics"],
  [true, definition, [{...association, pval: "1e-100"}], "invalid_association_statistics"],
  [true, definition, [{...association, beta: null}], "invalid_association_statistics"],
] as const)("keeps unavailable/ambiguous native joins explicit (%s)", (identity, selected, results, reason) => {
  expect(joinFinnGenPhenotype({results}, identity, "T2D", selected)).toMatchObject({joined: false, reason, association: null, score_contribution: 0});
});
