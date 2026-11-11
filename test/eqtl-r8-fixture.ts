import { hashCanonicalJson } from "@bio-mcp/shared";
import { EQTL_R8_CONTEXT_VERSION, type EqtlR8Input } from "../src/lib/eqtl-r8-input";
import { r8Request, type EqtlR8Observation } from "../src/lib/eqtl-r8-context";

export const r8Input: EqtlR8Input = { release: "r8_beta", dataset_id: "QTD000116", rsid: "rs1", metadata_sha256: "a".repeat(64),
  file_sha256: "b".repeat(64), max_rows: 200, score_fine_mapping: true };
export const r8Anchor = { rsid: "rs1", locus_id: "L1", grch38: { chr: "1", pos: 10, ref: "A", alt: "G" } };
export const r8StagedAnchor = { rsid: "rs1", grch38_chr: "1", grch38_pos: 10, grch38_ref: "A", grch38_alt: "G" };
export const r8Caps = JSON.stringify({ eqtl_catalogue_r8: r8Input, eqtl_catalogue_r8_context_version: EQTL_R8_CONTEXT_VERSION });
export function r8Native() {
  const row: Record<string, unknown> = { gene_id: "ENSG00000225880", molecular_trait_id: "ENSG00000225880", cs_id: "ENSG00000225880_L1",
    variant: "chr1_10_A_G", chromosome: "1", position: 10, ref: "A", alt: "G", pip: 0.99, cs_min_r2: 1, cs_size: 1,
    beta: 0.76, se: 0.12, pvalue: 7e-10, rsid: "rs1" };
  return { ...r8Request(r8Input, "chr1_10_A_G").request, assembly: "GRCh38", status: "pre-release",
    source_url: "https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/r8_beta/susie/QTS000015/QTD000116/QTD000116.credible_sets.parquet",
    metadata: { dataset_id: "QTD000116", study_id: "QTS000015", study_label: "GTEx_v10", sample_group: "adipose_subcutaneous",
      tissue_id: "UBERON_0001013", tissue_label: "adipose", condition_label: "naive", quant_method: "ge" },
    schema: Object.entries(row).map(([name, value]) => ({ name, type: typeof value === "string" ? "string" : "double", nullable: true })),
    rows: [row], row_count: 1, truncated: false, truncation_reason: null, total_matching_rows: null };
}
export async function r8Observation(native = r8Native()): Promise<EqtlR8Observation> {
  return { rsid: "rs1", locus_id: "L1", requested_variant: "chr1_10_A_G", requested_dataset_id: r8Input.dataset_id,
    release_requested: "r8_beta", assembly_requested: "GRCh38", metadata_sha256_requested: r8Input.metadata_sha256,
    file_sha256_requested: r8Input.file_sha256, source_url: native.source_url, retrieved_at: "2026-10-06T00:00:00Z",
    native_response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native), status: "observed", error: null, coverage_complete: false };
}
