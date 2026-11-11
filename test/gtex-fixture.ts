import { hashCanonicalJson } from "@bio-mcp/shared";
import type { EqtlRow } from "../src/lib/gtex-evidence";

export async function gtexFixture(dataset = "gtex_v10" as "gtex_v8" | "gtex_v10") {
  const native = { variantId: "chr1_10_A_G_b38", geneSymbol: "GENE", gencodeId: "ENSG1.7", tissueSiteDetailId: "Liver", pValue: 0, nes: -0.4, extra: { maf: 0.15 } };
  const response = { data: [native], paging_info: { page: 0, totalNumberOfItems: 100 } };
  const hash = await hashCanonicalJson(response);
  const request = { variantId: [native.variantId], datasetId: dataset, page: 0, itemsPerPage: 250 };
  const source = new URL("https://gtexportal.org/api/v2/association/singleTissueEqtl");
  for (const [key, value] of Object.entries(request)) for (const item of Array.isArray(value) ? value : [value]) source.searchParams.append(key, String(item));
  const row: EqtlRow = { lead_rsid: "rs1", ensembl_id: "ENSG1", symbol: "GENE", gencode_id: native.gencodeId, tissue: "Liver",
    variant_id: native.variantId, genome_assembly: "GRCh38", dataset_id_requested: dataset, source_response_sha256: hash,
    source_row_index: 0, native_row_json: JSON.stringify(native), p_value: 0, slope: -0.4 };
  const observation: Record<string, unknown> = { usable: 1, dataset_id_requested: dataset, response_json: JSON.stringify(response),
    request_json: JSON.stringify(request), response_sha256: hash, source_url: source.href, http_status: 200,
    retrieved_at: "2026-10-01T00:00:00Z", error: null, release_pinned: 0, coverage_complete: 0 };
  const anchor: Record<string, unknown> = { rsid: "rs1", locus_id: "L1", p_value: 1e-10, grch38_chr: "1", grch38_pos: 10, grch38_ref: "A", grch38_alt: "G" };
  return { row, observation, anchor, native, response, dataset };
}
