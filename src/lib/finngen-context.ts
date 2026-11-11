import { hashCanonicalJson } from "@bio-mcp/shared";
import { boundedJsonFetch } from "./bounded-json-fetch";

export type FinnGenRelease = "R12" | "R13";
interface Anchor {
  rsid: string;
  locus_id?: string;
  grch38?: { chr: string; pos: number; ref: string | null; alt: string | null } | null;
}
export interface FinnGenObservation {
  rsid: string; locus_id: string | null; requested_variant: string | null;
  release_requested: FinnGenRelease; assembly_requested: "GRCh38";
  source_url: string | null; retrieved_at: string; http_status: number | null;
  native_response_json: string | null; response_sha256: string | null;
  status: "observed" | "error" | "skipped"; error: string | null;
  context_only: true; identity_verified: false; coverage_complete: false;
  release_basis: "requested_host"; effect_allele_basis: string;
}
const boundedFetch = boundedJsonFetch("FinnGen", 1024 * 1024, 4000);

export function finnGenVariantId(anchor: Anchor): string | null {
  const c = anchor.grch38;
  if (!c || !/^(?:[1-9]|1\d|2[0-2]|X|Y)$/.test(c.chr) || !Number.isSafeInteger(c.pos) || c.pos < 1 ||
    !c.ref || !c.alt || !/^[ACGT]+$/.test(c.ref) || !/^[ACGT]+$/.test(c.alt) || c.ref === c.alt) return null;
  return `${c.chr}:${c.pos}-${c.ref}-${c.alt}`;
}

function observation(anchor: Anchor, release: FinnGenRelease): FinnGenObservation {
  const variant = finnGenVariantId(anchor);
  return {
    rsid: anchor.rsid, locus_id: anchor.locus_id ?? null, requested_variant: variant,
    release_requested: release, assembly_requested: "GRCh38",
    source_url: variant ? `https://${release.toLowerCase()}.finngen.fi/api/variant/${encodeURIComponent(variant)}` : null,
    retrieved_at: new Date().toISOString(), http_status: null, native_response_json: null, response_sha256: null,
    status: "skipped", error: null, context_only: true, identity_verified: false, coverage_complete: false,
    release_basis: "requested_host", effect_allele_basis: "Provider documents effects relative to the reference-genome alternate allele; native effects are not transformed.",
  };
}

async function observe(anchor: Anchor, release: FinnGenRelease): Promise<FinnGenObservation> {
  const row = observation(anchor, release);
  if (!row.source_url) { row.error = "No resolved biallelic GRCh38 coordinate; no rsID or cross-build fallback"; return row; }
  const controller = new AbortController(); const timeout = setTimeout(() => controller.abort(), 4000);
  try {
    const response = await boundedFetch(row.source_url, { headers: { Accept: "application/json" }, redirect: "manual", signal: controller.signal });
    row.http_status = response.status;
    const native: unknown = await response.json();
    row.native_response_json = JSON.stringify(native); row.response_sha256 = await hashCanonicalJson(native);
    if (response.status !== 200 || response.headers.has("content-range")) throw new Error(`FinnGen HTTP ${response.status}; complete HTTP 200 required; native JSON retained`);
    if (!native || typeof native !== "object" || Array.isArray(native) || !("results" in native) || !Array.isArray(native.results)) {
      throw new Error("No native association results array; inspect retained response for QC, missing-variant or provider error details");
    }
    row.status = "observed";
  } catch (error) {
    row.status = "error"; row.error = error instanceof Error ? error.message.slice(0, 500) : "FinnGen request failed";
  } finally { clearTimeout(timeout); row.retrieved_at = new Date().toISOString(); }
  return row;
}

/** Variant-wide context only: no phenotype-to-EFO mapping or gene-score contribution. */
export async function fetchFinnGenContext(anchors: Anchor[], release: FinnGenRelease, warnings: string[]) {
  if (release !== "R12" && release !== "R13") throw new Error("finngen_release must be R12 or R13");
  if (anchors.length > 25) throw new Error("FinnGen context supports at most 25 anchors");
  const rows = await Promise.all(anchors.map(anchor => observe(anchor, release)));
  for (const row of rows) if (row.error) warnings.push(`FinnGen ${release} ${row.rsid}: ${row.error}`);
  warnings.push(`FinnGen ${release} observations are variant context; synthesis checks native identity and any explicitly selected phenocode. Phenotype equivalence and coverage remain unverified and are not qualified for L2G scoring. Release reflects the requested host, not an immutable snapshot.`);
  return rows;
}
