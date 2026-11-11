import { hashCanonicalJson } from "@bio-mcp/shared";
import { readCompleteTable } from "./complete-table";
import { bestGenebassAssociation } from "./genebass-context";
import { GENEBASS_CONTEXT_VERSION, genebassSourceUrl, type GenebassObservation } from "./genebass-observation";

/** Attach request-scoped cross-phenotype observations without asserting gene/trait matching. */
export async function loadGenebassContext(namespace: DurableObjectNamespace, id: string, capsJson?: string | null, wasCollected = true) {
  const context = new Map<string, Record<string, unknown>[]>();
  const caps = JSON.parse(capsJson ?? "{}");
  if (!wasCollected || caps.genebass_context_version === undefined) return context;
  if (![1, GENEBASS_CONTEXT_VERSION].includes(caps.genebass_context_version)) throw new Error("Unrecognized staged Genebass context version");
  const rows = await readCompleteTable<GenebassObservation>(namespace, id, "genebass_evidence");
  if (rows.length > 100) throw new Error("Genebass context exceeds the 100-request bound");
  for (const row of rows) {
    if (typeof row.requested_ensembl_id !== "string" || !row.requested_ensembl_id || typeof row.burden_set !== "string" || !row.burden_set ||
        !["observed", "error"].includes(row.status) || row.source_url !== genebassSourceUrl(row.requested_ensembl_id, row.burden_set, caps.genebass_context_version)) {
      throw new Error("Invalid staged Genebass request context");
    }
    const { native_response_json, ...reference } = row;
    let summary = null;
    if (native_response_json !== null) {
      const native = JSON.parse(native_response_json);
      if (await hashCanonicalJson(native) !== row.response_sha256) throw new Error("Staged Genebass source hash mismatch");
      if (row.status === "observed") {
        if (row.http_status !== 200) throw new Error("Staged Genebass HTTP status mismatch");
        summary = bestGenebassAssociation(native);
      }
    } else if (row.status === "observed") throw new Error("Observed Genebass context is missing native evidence");
    const refs = context.get(row.requested_ensembl_id) ?? [];
    refs.push({ ...reference, summary, context_only: true, identity_verified: false, trait_matched: false,
      release_pinned: false, contract_verified: false, source_data_access_id: id, source_table: "genebass_evidence" });
    context.set(row.requested_ensembl_id, refs);
  }
  return context;
}
