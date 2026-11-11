import { hashCanonicalJson } from "@bio-mcp/shared";
import { readCompleteTable } from "./complete-table";
import { GNOMAD_CONSTRAINT_PROVENANCE, GNOMAD_CONTEXT_VERSION, GNOMAD_GENE_QUERY, type GnomadRow } from "./gnomad-context";
import { gnomadProjection } from "./gnomad-projection";

/** Recheck native evidence and derive summaries again; retain source records in SQL. */
export async function loadGnomadContext(namespace: DurableObjectNamespace, id: string, capsJson?: string | null, wasCollected = true) {
  const context = new Map<string, Record<string, unknown>>();
  const caps = JSON.parse(capsJson ?? "{}");
  if (caps?.include_gnomad_context !== true || !wasCollected) return context;
  // Older gathers already retained the same request and native response fields.
  if (caps.gnomad_context_version !== undefined && caps.gnomad_context_version !== GNOMAD_CONTEXT_VERSION) throw new Error("Unrecognized staged gnomAD context version");
  const rows = await readCompleteTable<Omit<GnomadRow, "usable"> & { usable: boolean | number }>(namespace, id, "gnomad");
  if (rows.length === 0 || rows.length > 40) throw new Error("Collected gnomAD context requires 1–40 distinct gene observations");
  for (const row of rows) {
    const { requested, projection, usable } = await validateGnomadObservation(row, context);
    context.set(requested, {
      ...projection, requested_ensembl_id: requested, source_url: row.source_url, retrieved_at: row.retrieved_at,
      response_sha256: row.response_sha256, query: row.query, variables_json: row.variables_json,
      reference_genome_requested: row.reference_genome_requested, error: row.error,
      constraint_provenance_json: JSON.stringify(GNOMAD_CONSTRAINT_PROVENANCE),
      status: usable ? "observed" : "error", identity_verified: usable, context_only: true,
      coverage_complete: false, release_pinned: false, source_data_access_id: id, source_table: "gnomad",
    });
  }
  return context;
}

async function validateGnomadObservation(row: Omit<GnomadRow, "usable"> & { usable: boolean | number; }, context: Map<string, Record<string, unknown>>) {
  const requested = row.requested_ensembl_id;
  if (typeof requested !== "string" || !requested || context.has(requested)) throw new Error("Invalid or duplicate staged gnomAD gene observation");
  if (![true, false, 0, 1].includes(row.usable)) throw new Error("Invalid staged gnomAD observation status");
  const usable = row.usable === true || row.usable === 1;
  const variables = JSON.parse(row.variables_json);
  if (row.source_url !== "https://gnomad.broadinstitute.org/api" || row.query !== GNOMAD_GENE_QUERY || row.reference_genome_requested !== "GRCh38"
    || variables?.id !== requested || variables?.ref !== "GRCh38" || Object.keys(variables).length !== 2) throw new Error("Staged gnomAD source/request mismatch");
  if (typeof row.retrieved_at !== "string" || !Number.isFinite(Date.parse(row.retrieved_at))) throw new Error("Invalid staged gnomAD retrieval time");
  let native: unknown = null;
  if (typeof row.native_response_json === "string") {
    native = JSON.parse(row.native_response_json);
    if (await hashCanonicalJson(native) !== row.response_sha256) throw new Error("Staged gnomAD source hash mismatch");
  } else if (row.native_response_json !== null || row.response_sha256 !== null || usable) throw new Error("Staged gnomAD context is missing native evidence");
  if (usable ? row.error !== null : typeof row.error !== "string" || !row.error) throw new Error("Staged gnomAD error/status mismatch");
  const projection = usable ? gnomadProjection(native, requested) : {
    ensembl_id: null, symbol: null, reference_genome: null, canonical_transcript_id: null,
    mane_select_transcript_json: "null", gene_flags_json: "null", constraint_flags_json: "null",
    oe_lof: null, oe_lof_lower: null, oe_lof_upper: null, oe_lof_percentile: null, lof_z: null, mis_z: null, pli: null, constraint_available: null,
  };
  return { requested, projection, usable };
}
