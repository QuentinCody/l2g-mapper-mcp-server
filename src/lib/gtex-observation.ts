import { hashCanonicalJson } from "@bio-mcp/shared";
import { gtexGet } from "@bio-mcp/shared/adapters/gtex";
import { boundedJsonFetch } from "./bounded-json-fetch";

export type GtexDataset = "gtex_v8" | "gtex_v10";
export interface GtexObservation {
  source_url: string; dataset_id_requested: GtexDataset; request_json: string;
  response_json: string | null; response_sha256: string | null;
  native_text: string | null; http_status: number | null; retrieved_at: string;
  usable: boolean; error: string | null; coverage_complete: false; release_pinned: false;
}

/** Keep the complete returned page, including unmatched rows and provider paging metadata. */
export async function observeGtex(params: Record<string, unknown>, dataset: GtexDataset) {
  const source = new URL("https://gtexportal.org/api/v2/association/singleTissueEqtl");
  for (const [key, value] of Object.entries(params)) {
    for (const item of Array.isArray(value) ? value : [value]) source.searchParams.append(key, String(item));
  }
  const evidence: GtexObservation = {
    source_url: source.href, dataset_id_requested: dataset, request_json: JSON.stringify(params),
    response_json: null, response_sha256: null, native_text: null, http_status: null,
    retrieved_at: new Date().toISOString(), usable: false, error: null,
    coverage_complete: false, release_pinned: false,
  };
  const bounded = boundedJsonFetch("GTEx", 4 * 1024 * 1024, 4000);
  const fetchImpl: typeof fetch = async (input, init) => {
    const response = await bounded(input, { ...init, redirect: "manual" });
    evidence.http_status = response.status;
    evidence.native_text = await response.text();
    try {
      const native = JSON.parse(evidence.native_text);
      evidence.response_json = JSON.stringify(native);
      evidence.response_sha256 = await hashCanonicalJson(native);
    } catch { /* Non-JSON failures remain available as native_text. */ }
    return new Response(evidence.native_text, { status: response.status, headers: response.headers });
  };
  try {
    const response = await gtexGet<unknown>("/association/singleTissueEqtl", params, { fetchImpl, timeoutMs: 8000 });
    evidence.response_json = JSON.stringify(response);
    evidence.response_sha256 = await hashCanonicalJson(response);
    if (!response || typeof response !== "object" || !Array.isArray((response as { data?: unknown }).data)) {
      throw new Error("GTEx response has no association data array");
    }
    const object = response as { data: unknown[]; datasetId?: unknown };
    if (object.datasetId !== undefined && object.datasetId !== dataset) throw new Error("GTEx response dataset differs from the requested dataset");
    evidence.usable = true;
    return { rows: object.data, evidence };
  } catch (error) {
    evidence.error = error instanceof Error ? error.message.slice(0, 500) : "GTEx request failed";
    return { rows: [], evidence };
  }
}
