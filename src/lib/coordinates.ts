import type { GenomeBuild, VariantCoord } from "@bio-mcp/shared/variants/resolve";
import { boundedJsonFetch } from "./bounded-json-fetch";
import { selectEnsemblMapping, type MappingSelection } from "./ensembl-mapping";

export interface CoordinateObservation {
  requested_rsid: string;
  requested_assembly: GenomeBuild;
  request_url: string;
  retrieved_at: string | null;
  status: MappingSelection["status"] | "failed";
  selected_coord_json: string;
  native_response_json: string | null;
  error: string | null;
}

async function resolveBuild(rsid: string, build: GenomeBuild) {
  const host = build === "GRCh38" ? "https://rest.ensembl.org" : "https://grch37.rest.ensembl.org";
  const url = `${host}/variation/human/${encodeURIComponent(rsid)}?content-type=application/json`;
  const observation: CoordinateObservation = { requested_rsid: rsid, requested_assembly: build, request_url: url,
    retrieved_at: null, status: "failed", selected_coord_json: "null", native_response_json: null, error: null };
  const controller = new AbortController(); const timer = setTimeout(() => controller.abort(), 6000);
  try {
    if (!/^rs\d+$/.test(rsid)) throw new Error("Invalid rsID");
    const response = await boundedJsonFetch("Ensembl variation", 1024 * 1024, 6000)(url, {
      headers: { Accept: "application/json" }, signal: controller.signal, redirect: "manual",
    });
    if (!response.ok) throw new Error(`Ensembl HTTP ${response.status}`);
    const native: unknown = await response.json();
    if (controller.signal.aborted) throw new Error("Ensembl variation request timed out");
    const selection = selectEnsemblMapping(native, rsid, build);
    Object.assign(observation, { retrieved_at: new Date().toISOString(), status: selection.status,
      selected_coord_json: JSON.stringify(selection.coord), native_response_json: JSON.stringify(native) });
    return { coord: selection.coord, observation };
  } catch (error) {
    observation.error = error instanceof Error ? error.message : String(error);
    return { coord: null, observation };
  } finally { clearTimeout(timer); }
}

/** Assembly-specific native evidence is retained even when no allele is qualified. */
export async function resolveAnchor(rsid: string, warnings: string[] = []): Promise<{
  grch38: VariantCoord | null; grch37: VariantCoord | null; warnings: string[]; observations: CoordinateObservation[];
}> {
  const results = await Promise.all([resolveBuild(rsid, "GRCh38"), resolveBuild(rsid, "GRCh37")]);
  for (const { observation } of results) if (observation.status !== "resolved") {
    warnings.push(`${rsid} ${observation.requested_assembly}: ${observation.status}${observation.error ? ` (${observation.error})` : ""}; allele-specific evidence unavailable.`);
  }
  return { grch38: results[0].coord, grch37: results[1].coord, warnings, observations: results.map(result => result.observation) };
}
