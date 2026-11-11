import { afterEach, expect, it, vi } from "vitest";
import { gtexGet } from "@bio-mcp/shared/adapters/gtex";
import { fetchGtexEqtls } from "./gtex-evidence";

vi.mock("@bio-mcp/shared/adapters/gtex", () => ({ gtexGet: vi.fn() }));
afterEach(() => vi.resetAllMocks());
const anchors = [{ rsid: "rs1", grch38: { chr: "1", pos: 100, ref: "A", alt: "C" } }];

it("requests a named dataset and preserves exact variant, assembly and gene annotation", async () => {
  vi.mocked(gtexGet).mockResolvedValue({ data: [
    { variantId: "chr1_100_A_C_b38", geneSymbol: "GENE", gencodeId: "ENSG1.7", tissueSiteDetailId: "Liver", pValue: 1e-8, nes: -0.4 },
    { variantId: "chr1_100_A_G_b38", snpId: "rs1", geneSymbol: "OTHER_ALLELE" },
    { variantId: "chr1_100_A_C_b37", snpId: "rs1", geneSymbol: "OTHER_BUILD" },
    { snpId: "rs1", geneSymbol: "UNKNOWN_ALLELE" },
  ] });
  const warnings: string[] = [];
  const result = await fetchGtexEqtls(anchors, 200, warnings);
  expect(result.rows).toEqual([{
    lead_rsid: "rs1", variant_id: "chr1_100_A_C_b38", genome_assembly: "GRCh38", dataset_id_requested: "gtex_v8",
    gencode_id: "ENSG1.7", ensembl_id: "ENSG1", symbol: "GENE", tissue: "Liver", p_value: 1e-8, slope: -0.4,
    source_response_sha256: result.evidence[0].response_sha256, source_row_index: 0,
    native_row_json: JSON.stringify({ variantId: "chr1_100_A_C_b38", geneSymbol: "GENE", gencodeId: "ENSG1.7", tissueSiteDetailId: "Liver", pValue: 1e-8, nes: -0.4 }),
  }]);
  expect(gtexGet).toHaveBeenCalledWith("/association/singleTissueEqtl", { variantId: ["chr1_100_A_C_b38"], datasetId: "gtex_v8", page: 0, itemsPerPage: 200 }, { fetchImpl: expect.any(Function), timeoutMs: 8000 });
  expect(warnings.join(" ")).toContain("coverage and source snapshot are not verified");
});

it("skips unresolved or ambiguous alleles without querying", async () => {
  expect(await fetchGtexEqtls([{ rsid: "rs1", grch38: { chr: "1", pos: 100, ref: "A", alt: null } }], 200, [])).toEqual({ rows: [], evidence: [] });
  expect(gtexGet).not.toHaveBeenCalled();
});

it("returns a source warning rather than treating a failed request as verified absence", async () => {
  vi.mocked(gtexGet).mockRejectedValue(new Error("HTTP 503"));
  const warnings: string[] = [];
  const result = await fetchGtexEqtls(anchors, 200, warnings);
  expect(result.rows).toEqual([]);
  expect(result.evidence[0]).toMatchObject({ usable: false, error: "HTTP 503", http_status: null });
  expect(warnings).toEqual(["GTEx eQTL batch failed: HTTP 503"]);
});
