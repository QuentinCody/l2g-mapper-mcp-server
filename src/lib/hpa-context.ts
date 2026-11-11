import { hashCanonicalJson } from "@bio-mcp/shared";
import { boundedJsonFetch } from "./bounded-json-fetch";
import { hpaProjection } from "./hpa-projection";

export const HPA_CONTEXT_VERSION = 1;
export interface HpaRow {
  symbol: string; ensembl_id: string | null; tissues: string; subcellular: string | null;
  requested_ensembl_id: string; observed_symbol: string | null;
  source_url: string | null; retrieved_at: string; http_status: number | null;
  native_response_json: string | null; response_sha256: string | null;
  status: "observed" | "error" | "skipped"; error: string | null;
  context_only: true; identity_verified: boolean; coverage_complete: false; release_pinned: false;
  rna_tissue_specific_ntpm_json: string; subcellular_location_json: string;
}
const boundedFetch = boundedJsonFetch("HPA", 1024 * 1024, 4000);

async function observe(symbol: string, id: string): Promise<HpaRow> {
  const valid = /^ENSG\d{11}$/.test(id);
  const row: HpaRow = {
    symbol, requested_ensembl_id: id, ensembl_id: null, observed_symbol: null, tissues: "", subcellular: null,
    source_url: valid ? `https://www.proteinatlas.org/${id}.json` : null,
    retrieved_at: new Date().toISOString(), http_status: null, native_response_json: null, response_sha256: null,
    status: "skipped", error: null, context_only: true, identity_verified: false, coverage_complete: false, release_pinned: false,
    rna_tissue_specific_ntpm_json: "null", subcellular_location_json: "null",
  };
  if (!row.source_url) { row.error = "No stable human Ensembl gene ID; no symbol fallback"; return row; }
  const controller = new AbortController(); const timeout = setTimeout(() => controller.abort(), 4000);
  try {
    const response = await boundedFetch(row.source_url, { headers: { Accept: "application/json" }, redirect: "manual", signal: controller.signal });
    row.http_status = response.status;
    const native: unknown = await response.json();
    row.native_response_json = JSON.stringify(native); row.response_sha256 = await hashCanonicalJson(native);
    if (!response.ok) throw new Error(`HPA HTTP ${response.status}; native JSON retained`);
    Object.assign(row, hpaProjection(native, id), { status: "observed", identity_verified: true });
  } catch (error) {
    row.status = "error"; row.error = error instanceof Error ? error.message.slice(0, 500) : "HPA request failed";
  } finally { clearTimeout(timeout); row.retrieved_at = new Date().toISOString(); }
  return row;
}

/** Optional expression/location context; no causal or protein-abundance scoring claim. */
export async function fetchHpaLane(symbolToEnsembl: Map<string, string>, warnings: string[]): Promise<HpaRow[]> {
  if (symbolToEnsembl.size > 20) warnings.push("HPA context capped at 20 candidate genes; coverage is incomplete");
  const rows = await Promise.all([...symbolToEnsembl.entries()].slice(0, 20).map(([symbol, id]) => observe(symbol, id)));
  for (const row of rows) if (row.error) warnings.push(`HPA ${row.symbol}/${row.requested_ensembl_id}: ${row.error}`);
  warnings.push("HPA RNA tissue-specific nTPM is a specificity summary, not a complete tissue profile or protein abundance. Display keeps at most eight tissues at nTPM >= 1; all native values remain staged. HPA context does not affect L2G scores; release is not pinned.");
  return rows;
}
