import { hashCanonicalJson } from "@bio-mcp/shared";
import { z } from "zod";
import { boundedJsonFetch } from "./bounded-json-fetch";
import { readCompleteTable } from "./complete-table";
import type { FinnGenRelease } from "./finngen-context";

export const finnGenPhenocode = z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$/);
export const FINNGEN_JOIN_VERSION = "2026-10-06-native-phenocode";
export interface FinnGenPhenotypeObservation {
  requested_phenocode: string; release_requested: FinnGenRelease; source_url: string;
  retrieved_at: string; http_status: number | null; native_response_json: string | null;
  response_sha256: string | null; status: "observed" | "error"; error: string | null;
}
const fetchBounded = boundedJsonFetch("FinnGen phenotype", 256 * 1024, 4000);
const url = (release: string, code: string) => `https://${release.toLowerCase()}.finngen.fi/api/pheno/${encodeURIComponent(code)}`;
export const finnGenRecord = (value: unknown): Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};

/** One native definition per gather; never infer a phenocode from an EFO label. */
export async function fetchFinnGenPhenotype(release: FinnGenRelease, code: string): Promise<FinnGenPhenotypeObservation[]> {
  finnGenPhenocode.parse(code);
  if (release !== "R12" && release !== "R13") throw new Error("Unknown FinnGen release");
  const observation: FinnGenPhenotypeObservation = {requested_phenocode: code, release_requested: release,
    source_url: url(release, code), retrieved_at: new Date().toISOString(), http_status: null,
    native_response_json: null, response_sha256: null, status: "error", error: null};
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 4000);
  try {
    const response = await fetchBounded(observation.source_url, {headers: {Accept: "application/json"}, redirect: "manual", signal: controller.signal});
    observation.http_status = response.status;
    const native: unknown = await response.json();
    observation.native_response_json = JSON.stringify(native);
    observation.response_sha256 = await hashCanonicalJson(native);
    if (response.status !== 200 || response.headers.has("content-range")) throw new Error(`FinnGen phenotype HTTP ${response.status}; complete HTTP 200 required`);
    const definition = finnGenRecord(native);
    if (definition.phenocode !== code || typeof definition.phenostring !== "string" || !definition.phenostring.trim()) throw new Error("FinnGen phenotype definition identity mismatch");
    observation.status = "observed";
  } catch (error) {
    observation.error = error instanceof Error ? error.message.slice(0, 500) : "FinnGen phenotype request failed";
  } finally { clearTimeout(timer); observation.retrieved_at = new Date().toISOString(); }
  return [observation];
}

/** Source hashes and the exact release/phenocode request are rechecked at synthesis. */
export async function loadFinnGenPhenotype(namespace: DurableObjectNamespace, id: string, release: string, code: string, version: unknown) {
  finnGenPhenocode.parse(code);
  if (version !== FINNGEN_JOIN_VERSION) throw new Error("Unsupported FinnGen phenotype join version; re-gather");
  const rows = await readCompleteTable<FinnGenPhenotypeObservation>(namespace, id, "finngen_phenotype_evidence");
  if (rows.length !== 1) throw new Error("Exactly one FinnGen phenotype definition observation is required");
  const row = rows[0];
  if (row.requested_phenocode !== code || row.release_requested !== release || row.source_url !== url(release, code) ||
      !Number.isFinite(Date.parse(row.retrieved_at)) || !["observed", "error"].includes(row.status) ||
      (row.http_status !== null && (!Number.isInteger(row.http_status) || row.http_status < 100 || row.http_status > 599)) ||
      (row.status === "observed" ? row.http_status !== 200 || row.error !== null : typeof row.error !== "string" || !row.error)) {
    throw new Error("Staged FinnGen phenotype request/status mismatch");
  }
  let native: Record<string, unknown> | null = null;
  if (row.native_response_json !== null) {
    if (typeof row.native_response_json !== "string" || row.http_status === null) throw new Error("Invalid FinnGen phenotype source evidence");
    const decoded: unknown = JSON.parse(row.native_response_json);
    if (await hashCanonicalJson(decoded) !== row.response_sha256) throw new Error("Staged FinnGen phenotype hash mismatch");
    if (row.status === "observed") {
      native = finnGenRecord(decoded);
      if (native.phenocode !== code || typeof native.phenostring !== "string" || !native.phenostring.trim()) throw new Error("Staged FinnGen phenotype identity mismatch");
    }
  } else if (row.status === "observed" || row.response_sha256 !== null) throw new Error("Missing FinnGen phenotype source evidence");
  const {native_response_json: _native, ...reference} = row;
  return {native, reference: {...reference, source_data_access_id: id, source_table: "finngen_phenotype_evidence"}};
}
