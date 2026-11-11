import { hashCanonicalJson } from "@bio-mcp/shared";
import { readCompleteTable } from "./complete-table";
import { eqtlAnchorVariant } from "./eqtl-catalogue-context";
import type { StagedAnchor } from "./eqtl-catalogue-synthesis";
import { checkedR8Native, type EqtlR8Observation, type EqtlR8Native } from "./eqtl-r8-context";
import { assertCatalogueSelection, EQTL_R8_CONTEXT_VERSION, eqtlR8Schema } from "./eqtl-r8-input";

export async function loadEqtlR8Observation(namespace: DurableObjectNamespace, id: string, capsJson: string | null | undefined, anchors: StagedAnchor[]) {
  const caps = JSON.parse(capsJson ?? "{}");
  assertCatalogueSelection(caps);
  if (caps.eqtl_catalogue_r8 == null) return null;
  const input = eqtlR8Schema.parse(caps.eqtl_catalogue_r8);
  if (caps.eqtl_catalogue_r8_context_version !== EQTL_R8_CONTEXT_VERSION) throw new Error("Unsupported r8 Catalogue context version");
  const rows = await readCompleteTable<EqtlR8Observation>(namespace, id, "eqtl_catalogue_r8_evidence");
  if (rows.length !== 1) throw new Error("Expected one explicit r8 Catalogue observation");
  const row = rows[0];
  if (row.rsid !== input.rsid || row.requested_dataset_id !== input.dataset_id || row.release_requested !== "r8_beta" ||
    row.assembly_requested !== "GRCh38" || row.metadata_sha256_requested !== input.metadata_sha256 ||
    row.file_sha256_requested !== input.file_sha256 || !["observed", "error", "skipped"].includes(row.status)) throw new Error("Staged r8 Catalogue request mismatch");
  const selected = anchors.filter(a => a.rsid === row.rsid);
  const a = selected.length === 1 ? selected[0] : undefined;
  const variant = a ? eqtlAnchorVariant({ rsid: a.rsid, grch38: { chr: a.grch38_chr ?? "", pos: a.grch38_pos ?? 0,
    ref: a.grch38_ref ?? null, alt: a.grch38_alt ?? null } }) : null;
  if (row.requested_variant !== (variant ? `chr${variant}` : null)) throw new Error("Staged r8 Catalogue anchor mismatch");
  let native: EqtlR8Native | null = null;
  if (row.native_response_json !== null) {
    const value: unknown = JSON.parse(row.native_response_json);
    if (await hashCanonicalJson(value) !== row.response_sha256) throw new Error("Staged r8 Catalogue hash mismatch");
    if (row.status !== "observed" || row.error !== null || !row.requested_variant) throw new Error("Staged r8 Catalogue observation mismatch");
    native = checkedR8Native(value, input, row.requested_variant);
    if (row.source_url !== native.source_url) throw new Error("Staged r8 Catalogue source mismatch");
  } else if (row.status === "observed" || row.response_sha256 !== null || row.source_url !== null || typeof row.error !== "string" || !row.error ||
    (row.status === "skipped") !== (variant === null)) throw new Error("Staged r8 Catalogue observation lacks native evidence");
  return { row, input, native };
}

export function r8SynthesisContext(checked: Awaited<ReturnType<typeof loadEqtlR8Observation>>, id: string) {
  const context = new Map<string, Record<string, unknown>[]>();
  if (!checked) return context;
  const { row, input, native } = checked;
  context.set(row.rsid, [{ ...row, native_response_json: undefined, source_data_access_id: id, source_table: "eqtl_catalogue_r8_evidence",
    dataset: native?.metadata ?? null, schema: native?.schema ?? null, allele_matching_row_count: native?.rows.length ?? 0,
    truncated: native?.truncated ?? null, score_fine_mapping_requested: input.score_fine_mapping,
    release_status: "pre-release", coverage_complete: false, effect_allele_basis: "ALT; native effects unchanged",
    evidence_type: "fine-mapped molecular cis-QTL; not trait colocalisation or independent replication" }]);
  return context;
}
