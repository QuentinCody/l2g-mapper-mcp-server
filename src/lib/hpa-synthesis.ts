import { hashCanonicalJson } from "@bio-mcp/shared";
import { readCompleteTable } from "./complete-table";
import { HPA_CONTEXT_VERSION, type HpaRow } from "./hpa-context";
import { hpaProjection } from "./hpa-projection";

/** Validate native identity/hash and rederive context; large source records remain staged. */
export async function loadHpaContext(namespace: DurableObjectNamespace, id: string, capsJson?: string | null, wasCollected = true) {
  const context = new Map<string, Record<string, unknown>[]>();
  const caps = JSON.parse(capsJson ?? "{}");
  if (caps.include_hpa_tissue_context !== true || !wasCollected || caps.hpa_context_version === undefined) return context;
  if (caps.hpa_context_version !== HPA_CONTEXT_VERSION) throw new Error("Unrecognized staged HPA context version");
  const rows = await readCompleteTable<HpaRow>(namespace, id, "hpa");
  if (rows.length > 20) throw new Error("HPA context exceeds the declared 20-gene bound");
  for (const row of rows) {
    if (typeof row.requested_ensembl_id !== "string" || !["observed", "error", "skipped"].includes(row.status)) throw new Error("Invalid staged HPA observation");
    const { native_response_json, ...reference } = row;
    if (native_response_json !== null) {
      const native = JSON.parse(native_response_json);
      if (await hashCanonicalJson(native) !== row.response_sha256) throw new Error("Staged HPA source hash mismatch");
      if (row.status === "observed") {
        if (row.http_status !== 200 || row.source_url !== `https://www.proteinatlas.org/${row.requested_ensembl_id}.json`) throw new Error("Staged HPA source context mismatch");
        Object.assign(reference, hpaProjection(native, row.requested_ensembl_id), { identity_verified: true });
      }
    } else if (row.status === "observed") throw new Error("Observed HPA context is missing native evidence");
    if (row.status !== "observed") Object.assign(reference, { identity_verified: false, ensembl_id: null, tissues: "", subcellular: null, rna_tissue_specific_ntpm_json: "null", subcellular_location_json: "null" });
    const refs = context.get(row.requested_ensembl_id) ?? [];
    refs.push({ ...reference, context_only: true, coverage_complete: false, release_pinned: false, source_data_access_id: id, source_table: "hpa" });
    context.set(row.requested_ensembl_id, refs);
  }
  return context;
}
