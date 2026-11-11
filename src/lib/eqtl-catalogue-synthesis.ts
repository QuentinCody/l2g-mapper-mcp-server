import { hashCanonicalJson } from "@bio-mcp/shared";
import type { EqtlDataset } from "@bio-mcp/shared/adapters/eqtl/ftp-catalogue";
import type { RegionResult } from "@bio-mcp/shared/adapters/eqtl/sumstats";
import { assertR7Snapshot } from "@bio-mcp/shared/adapters/eqtl/r7-snapshot";
import { readCompleteTable } from "./complete-table";
import { eqtlAnchorVariant, matchingEqtlRows, type EqtlCatalogueObservation } from "./eqtl-catalogue-context";
import { EQTL_CATALOGUE_CONTEXT_VERSION, eqtlCatalogueSchema, type EqtlCatalogueInput } from "./eqtl-catalogue-input";
import { assertEqtlSource } from "./eqtl-catalogue-transport";
import { assertEqtlTable } from "./eqtl-table-evidence";
import { assertEqtlFineMapping, type EqtlFineMapping } from "./eqtl-fine-mapping";
import { assertCatalogueSelection } from "./eqtl-r8-input";

export interface StagedAnchor {
  rsid: string; grch38_chr?: string | null; grch38_pos?: number | null;
  grch38_ref?: string | null; grch38_alt?: string | null;
}

export interface EqtlCatalogueNative { dataset: EqtlDataset; region: RegionResult; fine_mapping?: EqtlFineMapping }

function checkFileEvidence(region: RegionResult) {
  for (const role of ["index", "header", "range"] as const) {
    const evidence = region.file_evidence?.[role];
    if (role === "range" && region.byte_range === null && evidence === null) continue;
    if (!evidence || evidence.source_url !== (role === "index" ? region.index_file : region.source_file) ||
      evidence.http_status !== (role === "index" ? 200 : 206) || evidence.hash_scope !== "complete_response_bytes" ||
      !/^[a-f0-9]{64}$/.test(evidence.sha256) || !Number.isSafeInteger(evidence.byte_length) || evidence.byte_length < 1) {
      throw new Error("Staged eQTL Catalogue file-response evidence mismatch");
    }
  }
}

async function checkedNative(row: EqtlCatalogueObservation, input: EqtlCatalogueInput) {
  if (row.native_response_json === null) {
    if (row.status === "observed" || row.response_sha256 !== null) throw new Error("eQTL Catalogue observation lacks native evidence");
    return null;
  }
  const native = JSON.parse(row.native_response_json) as EqtlCatalogueNative;
  if (await hashCanonicalJson(native) !== row.response_sha256) throw new Error("Staged eQTL Catalogue hash mismatch");
  const { dataset, region } = native;
  if (row.status !== "observed" || row.error !== null || !row.requested_variant || dataset.catalogue_snapshot?.release !== "r7" ||
    dataset.dataset_id !== row.requested_dataset_id || dataset.sumstats_url !== row.source_url ||
    region.source_file !== row.source_url || region.index_file !== dataset.sumstats_index_url || !Array.isArray(region.rows) ||
    !Array.isArray(region.raw_rows) || region.raw_rows.length !== region.rows.length) throw new Error("Staged eQTL Catalogue dataset/file evidence mismatch");
  assertEqtlSource(dataset.sumstats_url, dataset.dataset_id, dataset.study_id);
  if (dataset.sumstats_index_url !== `${dataset.sumstats_url}.tbi`) throw new Error("Staged eQTL Catalogue index mismatch");
  const [chr, pos] = row.requested_variant.split("_");
  if (region.chromosome.replace(/^chr/, "") !== chr || region.start !== Number(pos) || region.end !== Number(pos) ||
    region.row_count !== region.rows.length || region.rows.length > input.max_rows || region.coverage_complete !== false ||
    typeof region.truncated !== "boolean") throw new Error("Staged eQTL Catalogue point query mismatch");
  assertR7Snapshot(dataset.catalogue_snapshot, { metadata_sha256: row.metadata_sha256_requested, paths_sha256: row.paths_sha256_requested });
  checkFileEvidence(region);
  assertEqtlTable(region);
  assertEqtlFineMapping(native.fine_mapping, dataset, row.requested_variant, input);
  return native;
}

/** Share the same complete source and anchor validation with scoring and synthesis. */
export async function loadEqtlCatalogueObservation(namespace: DurableObjectNamespace, id: string, capsJson: string | null | undefined, anchors: StagedAnchor[]) {
  const caps = JSON.parse(capsJson ?? "{}");
  assertCatalogueSelection(caps);
  if (caps.eqtl_catalogue == null) return null;
  const input = eqtlCatalogueSchema.parse(caps.eqtl_catalogue);
  const legacyNominal = caps.eqtl_catalogue_context_version === "2026-10-01-r7-nominal" && !input.fine_mapping_sha256;
  if (caps.eqtl_catalogue_context_version !== EQTL_CATALOGUE_CONTEXT_VERSION && !legacyNominal) throw new Error("Unsupported eQTL Catalogue context version");
  const rows = await readCompleteTable<EqtlCatalogueObservation>(namespace, id, "eqtl_catalogue_evidence");
  if (rows.length !== 1) throw new Error("Expected one explicit eQTL Catalogue observation");
  const row = rows[0];
  if (row.rsid !== input.rsid || row.requested_dataset_id !== input.dataset_id || row.release_requested !== "r7" ||
    row.assembly_requested !== "GRCh38" || row.metadata_sha256_requested !== input.metadata_sha256 ||
    row.paths_sha256_requested !== input.paths_sha256 || !["observed", "error", "skipped"].includes(row.status)) throw new Error("Staged eQTL Catalogue request mismatch");
  const anchor = anchors.filter(a => a.rsid === row.rsid);
  const variant = anchor.length === 1 ? eqtlAnchorVariant({ rsid: row.rsid, grch38: { chr: anchor[0].grch38_chr ?? "",
    pos: anchor[0].grch38_pos ?? 0, ref: anchor[0].grch38_ref ?? null, alt: anchor[0].grch38_alt ?? null } }) : null;
  if (row.requested_variant !== variant) throw new Error("Staged eQTL Catalogue anchor mismatch");
  const native = await checkedNative(row, input);
  const indexes = native && variant ? matchingEqtlRows(native.region, variant) : [];
  return { row, input, native, variant, indexes };
}

/** Native observations remain in the gather; synthesis returns checked source references. */
export async function loadEqtlCatalogueContext(namespace: DurableObjectNamespace, id: string, capsJson: string | null | undefined, anchors: StagedAnchor[]) {
  const context = new Map<string, Record<string, unknown>[]>();
  const checked = await loadEqtlCatalogueObservation(namespace, id, capsJson, anchors);
  if (!checked) return context;
  const { row, input, native, variant, indexes } = checked;
  context.set(row.rsid, [{ rsid: row.rsid, requested_variant: variant, requested_dataset_id: input.dataset_id,
    release_requested: "r7", assembly_requested: "GRCh38", status: row.status, error: row.error,
    dataset: native?.dataset ?? null, file_evidence: native?.region.file_evidence ?? null,
    source_data_access_id: id, source_table: "eqtl_catalogue_evidence", source_url: row.source_url,
    response_sha256: row.response_sha256, retrieved_at: row.retrieved_at,
    matching_row_indexes: indexes, allele_matching_row_count: indexes.length,
    truncated: native?.region.truncated ?? null, context_only: true, coverage_complete: false,
    effect_allele_basis: "ALT; native effects unchanged", evidence_type: "nominal cis-QTL, not colocalisation or independent replication",
    fine_mapping_requested: Boolean(input.fine_mapping_sha256), fine_mapping_status: native?.fine_mapping?.status ?? null,
    fine_mapping_error: native?.fine_mapping?.error ?? null,
  }]);
  return context;
}
