import { opentargetsGraphql } from "@bio-mcp/shared/adapters/opentargets";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { boundedOpenTargetsFetch } from "./opentargets-http";

export interface OpenTargetsEvidenceRow {
  phase: string; source_url: string; query: string; variables_json: string;
  response_json: string | null; response_sha256: string | null; retrieved_at: string;
  data_version_json: string | null; api_version_json: string | null;
  usable: boolean; error: string | null;
}
export interface OtMeta { dataVersion?: { year?: unknown; month?: unknown; iteration?: unknown }; apiVersion?: unknown }
export const OT_META = "meta { name apiVersion { x y z suffix } dataVersion { year month iteration } }";

function releaseKey(meta: OtMeta | undefined): string {
  const version = meta?.dataVersion;
  if (!version || typeof version.year !== "string" || typeof version.month !== "string" || !version.year || !version.month) {
    throw new Error("Missing Open Targets data-release metadata; response excluded from ranking");
  }
  return JSON.stringify([version.year, version.month, version.iteration ?? null]);
}

async function capture<T>(entry: OpenTargetsEvidenceRow, query: string, variables: Record<string, unknown>, releases: Set<string>, budget: { bytes: number }) {
  const result = await opentargetsGraphql<T & { meta?: OtMeta }>(query, variables, { fetchImpl: boundedOpenTargetsFetch, timeoutMs: 4000 });
  const json = JSON.stringify(result);
  const bytes = new TextEncoder().encode(json).byteLength;
  if (budget.bytes + bytes > 12 * 1024 * 1024) throw new Error("OpenTargets native evidence exceeds the local 12 MiB gather budget; response excluded");
  budget.bytes += bytes;
  entry.response_json = json;
  entry.response_sha256 = await hashCanonicalJson(result);
  entry.retrieved_at = new Date().toISOString();
  const meta = result.data?.meta;
  entry.data_version_json = meta?.dataVersion ? JSON.stringify(meta.dataVersion) : null;
  entry.api_version_json = meta?.apiVersion ? JSON.stringify(meta.apiVersion) : null;
  if (result.errors?.length) throw new Error("GraphQL returned errors; inspect the native evidence");
  releases.add(releaseKey(meta));
  entry.usable = true;
  return result;
}

/** Capture partial failures as evidence; never infer a data release or model version. */
export function createOpenTargetsObservations(warnings: string[]) {
  const evidence: OpenTargetsEvidenceRow[] = [];
  const releases = new Set<string>();
  const budget = { bytes: 0 };
  async function request<T>(phase: string, query: string, variables: Record<string, unknown>) {
    const entry: OpenTargetsEvidenceRow = {
      phase, source_url: "https://api.platform.opentargets.org/api/v4/graphql", query,
      variables_json: JSON.stringify(variables), response_json: null, response_sha256: null,
      retrieved_at: new Date().toISOString(), data_version_json: null, api_version_json: null,
      usable: false, error: null,
    };
    evidence.push(entry);
    try { return await capture<T>(entry, query, variables, releases, budget); }
    catch (error) {
      entry.error = error instanceof Error ? error.message.slice(0, 500) : "Open Targets request failed";
      warnings.push(`OpenTargets ${phase}: ${entry.error}`);
      return { data: undefined };
    }
  }
  function consistent() {
    if (releases.size <= 1) return true;
    warnings.push("OpenTargets data release changed during gather; all Open Targets ranking rows excluded. Re-gather against a stable release.");
    for (const row of evidence) { row.usable = false; row.error ??= "Mixed data releases in gather"; }
    return false;
  }
  return { evidence, request, consistent };
}
