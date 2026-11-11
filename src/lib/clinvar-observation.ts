import { hashCanonicalJson } from "@bio-mcp/shared";
import { boundedJsonFetch } from "./bounded-json-fetch";

export const CLINVAR_CONTEXT_VERSION = 1;
export const CLINVAR_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/";
export interface ClinvarObservation {
  phase: "search" | "summary"; rsid: string | null; requested_ids_json: string;
  source_url: string | null; retrieved_at: string; http_status: number | null;
  native_response_json: string | null; response_sha256: string | null;
  status: "observed" | "error" | "skipped"; error: string | null;
  context_only: true; coverage_complete: false; release_pinned: false;
}
export function clinvarUrl(phase: ClinvarObservation["phase"], rsid: string | null, ids: string[]) {
  const url = new URL(phase === "search" ? "esearch.fcgi" : "esummary.fcgi", CLINVAR_BASE);
  url.searchParams.set("db", "clinvar"); url.searchParams.set("retmode", "json");
  if (phase === "search") {
    // NCBI documents bare rs identifiers for related-variant searches, not ClinVar variation IDs.
    url.searchParams.set("term", rsid ?? ""); url.searchParams.set("retmax", "3"); url.searchParams.set("retstart", "0");
  } else url.searchParams.set("id", ids.join(","));
  return url.href;
}
const boundedFetch = boundedJsonFetch("ClinVar", 4 * 1024 * 1024, 4000);
export async function observeClinvar(phase: ClinvarObservation["phase"], rsid: string | null, ids: string[], skip?: string): Promise<ClinvarObservation> {
  const row: ClinvarObservation = { phase, rsid, requested_ids_json: JSON.stringify(ids), source_url: null,
    retrieved_at: new Date().toISOString(), http_status: null, native_response_json: null, response_sha256: null,
    status: "skipped", error: skip ?? null, context_only: true, coverage_complete: false, release_pinned: false };
  if (skip) return row;
  row.source_url = clinvarUrl(phase, rsid, ids);
  const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), 4000);
  try {
    const response = await boundedFetch(row.source_url, { headers: { Accept: "application/json" }, redirect: "manual", signal: controller.signal });
    row.http_status = response.status;
    const native: unknown = await response.json();
    row.native_response_json = JSON.stringify(native); row.response_sha256 = await hashCanonicalJson(native);
    if (!response.ok) throw new Error(`ClinVar HTTP ${response.status}; native JSON retained`);
    row.status = "observed";
  } catch (error) { row.status = "error"; row.error = error instanceof Error ? error.message.slice(0, 500) : "ClinVar request failed"; }
  finally { clearTimeout(timer); row.retrieved_at = new Date().toISOString(); }
  return row;
}

export function object(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }
export function searchIds(native: unknown): string[] {
  if (!object(native) || native.error || !object(native.esearchresult)) throw new Error("Unrecognized ClinVar search response");
  const result = native.esearchresult;
  if (result.ERROR || !/^\d+$/.test(String(result.count)) || !Array.isArray(result.idlist) || result.idlist.length > 3 ||
      result.idlist.some(id => typeof id !== "string" || !/^[1-9]\d*$/.test(id)) || Number(result.count) < result.idlist.length ||
      new Set(result.idlist).size !== result.idlist.length) throw new Error("Invalid ClinVar search identifiers/count");
  return result.idlist as string[];
}
