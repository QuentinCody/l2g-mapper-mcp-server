import { hashCanonicalJson } from "@bio-mcp/shared";
import type { EqtlDataset } from "@bio-mcp/shared/adapters/eqtl/ftp-catalogue";
import type { RegionResult } from "@bio-mcp/shared/adapters/eqtl/sumstats";
import type { EqtlCatalogueObservation } from "../src/lib/eqtl-catalogue-context";
import { EQTL_CATALOGUE_CONTEXT_VERSION } from "../src/lib/eqtl-catalogue-input";
import type { EqtlCatalogueNative } from "../src/lib/eqtl-catalogue-synthesis";

export const eqtlInput = { release: "r7" as const, dataset_id: "QTD000021", rsid: "rs1", metadata_sha256: "a".repeat(64), paths_sha256: "b".repeat(64), max_rows: 200 };
export const eqtlCaps = JSON.stringify({ eqtl_catalogue: eqtlInput, eqtl_catalogue_context_version: EQTL_CATALOGUE_CONTEXT_VERSION });
export const eqtlAnchor = { rsid: "rs1", locus_id: "L1", grch38: { chr: "1", pos: 10, ref: "A", alt: "G" } };
export const eqtlStagedAnchor = { rsid: "rs1", locus_id: "L1", grch38_chr: "1", grch38_pos: 10, grch38_ref: "A", grch38_alt: "G" };
const url = "https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/sumstats/QTS000002/QTD000021/QTD000021.all.tsv.gz";

export function eqtlNative(): { dataset: EqtlDataset; region: RegionResult } {
  const evidence = { source_url: url, retrieved_at: "2026-10-01T00:00:00Z", http_status: 206, etag: "fixture", last_modified: null,
    content_range: "bytes 0-199/200", byte_length: 200, sha256: "c".repeat(64), hash_scope: "complete_response_bytes" as const };
  const row = { variant: "1_10_A_G", chromosome: "1", position: 10, ref: "A", alt: "G", rsid: "rs1", beta: -0.4, pvalue: 0 };
  return {
    dataset: { study_id: "QTS000002", dataset_id: eqtlInput.dataset_id, study_label: "BLUEPRINT", sample_group: "monocyte", tissue_id: "CL:0000576",
      tissue_label: "monocyte", condition_label: "naive", sample_size: 197, quant_method: "ge", study_type: "bulk", pmid: "27863251",
      sumstats_url: url, sumstats_index_url: `${url}.tbi`, permuted_url: null, credible_sets_url: null, lbf_url: null,
      catalogue_snapshot: { release: "r7", metadata_sha256: eqtlInput.metadata_sha256, paths_sha256: eqtlInput.paths_sha256, retrieved_at: evidence.retrieved_at } },
    region: { columns: Object.keys(row), rows: [row, { ...row, rsid: "rs2" }, { ...row, variant: "1_10_A_T", alt: "T" }], row_count: 3,
      native_header: Object.keys(row).join("\t"), raw_rows: ["1_10_A_G\t1\t10\tA\tG\trs1\t-0.4000\t1e-400", "1_10_A_G\t1\t10\tA\tG\trs2\t-0.4000\t1e-400", "1_10_A_T\t1\t10\tA\tT\trs1\t-0.4000\t1e-400"],
      file_evidence: { index: { ...evidence, source_url: `${url}.tbi`, http_status: 200, content_range: null }, header: evidence, range: evidence },
      coverage_complete: false, chromosome: "1", start: 10, end: 10, index_file: `${url}.tbi`, source_file: url,
      byte_range: { start: 0, end: 199 }, lines_scanned: 3, truncated: false },
  };
}

export async function eqtlObservation(native = eqtlNative()): Promise<EqtlCatalogueObservation> {
  return { rsid: "rs1", locus_id: "L1", requested_variant: "1_10_A_G", requested_dataset_id: eqtlInput.dataset_id,
    release_requested: "r7", assembly_requested: "GRCh38", metadata_sha256_requested: eqtlInput.metadata_sha256, paths_sha256_requested: eqtlInput.paths_sha256,
    source_url: native.dataset.sumstats_url, retrieved_at: "2026-10-01T00:00:00Z", native_response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native),
    status: "observed", error: null, allele_matching_row_count: 2, context_only: true, coverage_complete: false };
}

export function eqtlFineNative(): EqtlCatalogueNative {
  const native: EqtlCatalogueNative = eqtlNative();
  const gene = "ENSG00000134057";
  native.region.rows = native.region.rows.map(row => ({ ...row, gene_id: gene, molecular_trait_id: gene, se: 0.1 }));
  native.region.columns = Object.keys(native.region.rows[0]);
  native.region.native_header = native.region.columns.join("\t");
  native.region.raw_rows = native.region.rows.map(row => native.region.columns.map(c => row[c]).join("\t"));
  const url = "https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/susie/QTS000002/QTD000021/QTD000021.credible_sets.tsv.gz";
  native.dataset.credible_sets_url = url;
  const row = { gene_id: gene, molecular_trait_id: gene, variant: "chr1_10_A_G", cs_id: `${gene}_L1`, cs_size: 1, pip: 0.99, cs_min_r2: 1, pvalue: 0, beta: -0.4, se: 0.1 };
  const columns = Object.keys(row);
  native.fine_mapping = { requested_variant: row.variant, max_rows: 200, expected_sha256: "d".repeat(64), status: "observed", error: null,
    result: { columns, rows: [row], raw_rows: [Object.values(row).join("\t")], native_header: columns.join("\t"), row_count: 1,
      truncated: false, lines_scanned: 2, source_file: url, coverage_complete: false,
      file_evidence: { source_url: url, retrieved_at: "2026-10-06T00:00:00Z", http_status: 200, etag: null, last_modified: null,
        content_range: null, byte_length: 200, sha256: "d".repeat(64), hash_scope: "complete_response_bytes" } } };
  return native;
}
