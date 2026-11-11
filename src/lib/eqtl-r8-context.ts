import { hashCanonicalJson } from "@bio-mcp/shared";
import { parquetRequest, queryParquet, type ParquetEnv } from "@bio-mcp/shared/adapters/eqtl/parquet";
import { validateParquetResult } from "@bio-mcp/shared/adapters/eqtl/parquet-result";
import { eqtlAnchorVariant } from "./eqtl-catalogue-context";
import { eqtlR8Schema, type EqtlR8Input } from "./eqtl-r8-input";

type Anchor = Parameters<typeof eqtlAnchorVariant>[0];
export interface EqtlR8Observation {
  rsid: string; locus_id: string | null; requested_variant: string | null;
  requested_dataset_id: string; release_requested: "r8_beta"; assembly_requested: "GRCh38";
  metadata_sha256_requested: string; file_sha256_requested: string;
  source_url: string | null; retrieved_at: string;
  native_response_json: string | null; response_sha256: string | null;
  status: "observed" | "error" | "skipped"; error: string | null; coverage_complete: false;
}
export interface EqtlR8Native extends Record<string, unknown> {
  metadata: Record<string, unknown>; rows: Record<string, unknown>[];
  source_url: string; truncated: boolean;
}
export function r8Request(input: EqtlR8Input, variant: string) {
  const params = { release: input.release, metadata_sha256: input.metadata_sha256,
    file_sha256: input.file_sha256, variant, max_rows: input.max_rows };
  const route = { kind: "credible-sets", datasetId: input.dataset_id };
  return { route, params, request: parquetRequest(route, params) };
}

/** Recheck predicates as well as backend provenance; no rsID or strand fallback. */
export function checkedR8Native(value: unknown, input: EqtlR8Input, variant: string): EqtlR8Native {
  const native = validateParquetResult(value, r8Request(input, variant).request) as EqtlR8Native;
  const [chr, pos, ref, alt] = variant.replace(/^chr/, "").split("_");
  if (native.rows.some(row => row.variant !== variant || String(row.chromosome).replace(/^chr/, "") !== chr ||
    row.position !== Number(pos) || row.ref !== ref || row.alt !== alt)) throw new Error("r8 Catalogue result contains a mismatched allele or coordinate");
  return native;
}

/** One bounded query against an already imported, checksum-verified snapshot. */
export async function fetchEqtlR8Context(anchors: Anchor[], requested: EqtlR8Input | undefined, warnings: string[], env: ParquetEnv = {}) {
  if (!requested) return [];
  const input = eqtlR8Schema.parse(requested);
  const selected = anchors.filter(anchor => anchor.rsid === input.rsid);
  const anchor = selected.length === 1 ? selected[0] : undefined;
  const variant = anchor ? eqtlAnchorVariant(anchor) : null;
  const row: EqtlR8Observation = { rsid: input.rsid, locus_id: anchor?.locus_id ?? null, requested_variant: variant ? `chr${variant}` : null,
    requested_dataset_id: input.dataset_id, release_requested: "r8_beta", assembly_requested: "GRCh38",
    metadata_sha256_requested: input.metadata_sha256, file_sha256_requested: input.file_sha256,
    source_url: null, retrieved_at: new Date().toISOString(), native_response_json: null, response_sha256: null,
    status: "skipped", error: null, coverage_complete: false };
  if (!row.requested_variant) row.error = "Selected rsID is absent, ambiguous, or lacks an exact biallelic GRCh38 anchor";
  else {
    try {
      const { route, params } = r8Request(input, row.requested_variant);
      const native = checkedR8Native(await queryParquet(route, params, env), input, row.requested_variant);
      row.response_sha256 = await hashCanonicalJson(native);
      row.native_response_json = JSON.stringify(native);
      row.source_url = native.source_url;
      row.status = "observed";
    } catch (error) {
      row.status = "error";
      row.error = error instanceof Error ? error.message.slice(0, 500) : "r8 Catalogue read failed";
    }
  }
  row.retrieved_at = new Date().toISOString();
  if (row.error) warnings.push(`eQTL Catalogue r8 ${input.dataset_id} ${input.rsid}: ${row.error}`);
  warnings.push("r8_beta is pre-release. Dataset IDs are release-specific. Credible-set rows retain native association statistics; scoring requires explicit opt-in, PIP >= 0.95 and minimum r2 >= 0.25. This is not trait colocalisation, complete coverage or independent GTEx replication.");
  return [row];
}
