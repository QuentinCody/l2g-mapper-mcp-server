import { hashCanonicalJson } from "@bio-mcp/shared";
import { genebassGeneBurden } from "@bio-mcp/shared/adapters/genebass";
import { boundedJsonFetch } from "./bounded-json-fetch";

export const GENEBASS_CONTEXT_VERSION = 2;
export interface GenebassObservation {
  symbol: string; requested_ensembl_id: string; burden_set: string;
  source_url: string; retrieved_at: string; http_status: number | null;
  native_response_json: string | null; response_sha256: string | null;
  status: "observed" | "error"; error: string | null;
  context_only: true; identity_verified: false; trait_matched: false;
  release_pinned: false; contract_verified: false;
}

export function genebassSourceUrl(id: string, set: string, version = GENEBASS_CONTEXT_VERSION) {
  if (version === 2) {
    const url = new URL(`https://main.genebass.org/api/phewas/${encodeURIComponent(id)}`);
    url.searchParams.set("burdenSet", set);
    return url.toString();
  }
  const url = new URL("https://app.genebass.org/api/gene_burden_phewas");
  url.searchParams.set("gene_id", id); url.searchParams.set("burden_set", set);
  return url.toString();
}

// Leave space for request metadata beneath the DO SQLite row-size ceiling.
const boundedFetch = boundedJsonFetch("Genebass", 1_900_000, 4000);

/** Retain observations of the existing adapter; this does not qualify its live contract. */
export async function observeGenebass(symbol: string, id: string, set: string, skip?: string) {
  const row: GenebassObservation = {
    symbol, requested_ensembl_id: id, burden_set: set, source_url: genebassSourceUrl(id, set),
    retrieved_at: new Date().toISOString(), http_status: null, native_response_json: null, response_sha256: null,
    status: "error", error: null, context_only: true, identity_verified: false, trait_matched: false,
    release_pinned: false, contract_verified: false,
  };
  if (skip) return { ...row, error: skip };
  const retain = async (native: unknown) => {
    const serialized = JSON.stringify(native);
    if (typeof serialized !== "string") throw new Error("Genebass returned no JSON value");
    row.native_response_json = serialized; row.response_sha256 = await hashCanonicalJson(native);
  };
  try {
    const native = await genebassGeneBurden(id, set, { timeoutMs: 4000, fetchImpl: async (input, init) => {
      if (String(input) !== row.source_url) throw new Error("Unexpected Genebass adapter request URL");
      const response = await boundedFetch(input, { ...init, redirect: "manual" });
      row.http_status = response.status;
      if (!response.ok) {
        try { await retain(await response.clone().json()); } catch { /* Non-JSON error responses have no parsed native evidence. */ }
      }
      return response;
    } });
    await retain(native);
    row.status = "observed";
  } catch (error) {
    row.error = error instanceof Error ? error.message.slice(0, 500) : "Genebass request failed";
  } finally { row.retrieved_at = new Date().toISOString(); }
  return row;
}
