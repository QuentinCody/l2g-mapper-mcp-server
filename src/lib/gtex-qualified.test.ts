import { expect, it } from "vitest";
import { qualifiedGtex } from "./gtex-qualified";
import { gtexFixture } from "../../test/gtex-fixture";

it.each(["gtex_v8", "gtex_v10"] as const)("links %s tissue support to the complete native source without changing zero p-values or effect direction", async dataset => {
  const f = await gtexFixture(dataset);
  const result = await qualifiedGtex([f.row, f.row], [f.observation], [f.anchor], dataset, "saved-gather");
  expect(result.warnings).toEqual([]);
  expect([...result.support.keys()]).toEqual(['["rs1","ENSG1"]']);
  const support = [...result.support.values()][0];
  expect([...support.tissues]).toEqual(["Liver"]);
  expect(support.references).toEqual([{
    source_data_access_id: "saved-gather", source_table: "gtex_evidence", projection_table: "eqtl",
    response_sha256: f.row.source_response_sha256, source_row_index: 0,
    source_url: f.observation.source_url, retrieved_at: f.observation.retrieved_at,
    lead_rsid: "rs1", ensembl_id: "ENSG1", gencode_id: "ENSG1.7", variant_id: "chr1_10_A_G_b38",
    genome_assembly: "GRCh38", dataset_id_requested: dataset, tissue: "Liver", tissue_ontology_id: null, p_value: 0,
    normalized_effect_size: -0.4, coverage_complete: false, release_pinned: false,
  }]);
});

it.each([
  { ensembl_id: "ENSG2" }, { lead_rsid: "rs2" }, { tissue: "Brain" }, { genome_assembly: "GRCh37" },
  { variant_id: "chr1_10_A_C_b38" }, { dataset_id_requested: "gtex_v8" }, { source_response_sha256: "wrong" },
  { source_row_index: 1 }, { source_row_index: -1 }, { source_row_index: 0.5 }, { native_row_json: "{}" },
  { gencode_id: "ENSG1.8" }, { p_value: 0.1 }, { slope: 0.4 }, { tissue: "." },
])("excludes a projected association conflicting with native evidence: %j", async change => {
  const f = await gtexFixture();
  const result = await qualifiedGtex([{ ...f.row, ...change } as typeof f.row], [f.observation], [f.anchor], f.dataset, "saved");
  expect(result.support.size).toBe(0); expect(result.warnings[0]).toContain("1 GTEx rows");
});

it.each([
  { usable: 0 }, { http_status: 503 }, { error: "partial" }, { response_json: "invalid" },
  { response_sha256: "wrong" }, { request_json: '{}' }, { source_url: "https://example.org" },
  { dataset_id_requested: "gtex_v8" }, { retrieved_at: null },
])("excludes associations from unusable or inconsistent source pages: %j", async change => {
  const f = await gtexFixture();
  const result = await qualifiedGtex([f.row], [{ ...f.observation, ...change }], [f.anchor], f.dataset, "saved");
  expect(result.support.size).toBe(0); expect(result.warnings[0]).toContain("1 GTEx rows");
});

it.each([
  { anchors: [] }, { anchors: [{ rsid: "rs1" }] }, { anchors: [{ rsid: "rs1", grch38_chr: "1", grch38_pos: 10, grch38_ref: "A", grch38_alt: "C" }] },
])("requires the resolved allele from the same gathered anchor: %j", async ({ anchors }) => {
  const f = await gtexFixture();
  const result = await qualifiedGtex([f.row], [f.observation], anchors, f.dataset, "saved");
  expect(result.support.size).toBe(0);
});

it("does not assign source provenance to legacy projections or an unknown selected dataset", async () => {
  const f = await gtexFixture();
  const legacy = { lead_rsid: "rs1", ensembl_id: "ENSG1", tissue: "Liver" } as typeof f.row;
  for (const [rows, pages, dataset] of [[[legacy], [], f.dataset], [[f.row], [f.observation], undefined]] as const) {
    expect((await qualifiedGtex([...rows], [...pages], [f.anchor], dataset, "saved")).support.size).toBe(0);
  }
});
