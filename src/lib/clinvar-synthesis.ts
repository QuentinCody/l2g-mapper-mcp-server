import { hashCanonicalJson } from "@bio-mcp/shared";
import { readCompleteTable } from "./complete-table";
import { CLINVAR_CONTEXT_VERSION, clinvarUrl, searchIds, type ClinvarObservation } from "./clinvar-observation";
import { clinvarProjection } from "./clinvar-context";

async function validateObservation(row: ClinvarObservation) {
  if (!["search", "summary"].includes(row.phase) || !["observed", "error", "skipped"].includes(row.status)) throw new Error("Invalid staged ClinVar observation");
  const ids: unknown = JSON.parse(row.requested_ids_json);
  if (!Array.isArray(ids) || ids.length > 90 || ids.some(id => typeof id !== "string" || !/^[1-9]\d*$/.test(id))) throw new Error("Invalid staged ClinVar request IDs");
  if (row.phase === "search" ? typeof row.rsid !== "string" || ids.length !== 0 : row.rsid !== null || !ids.length) throw new Error("Invalid staged ClinVar request identity");
  if (row.status === "skipped") {
    if (row.source_url !== null || row.native_response_json !== null || row.response_sha256 !== null || row.http_status !== null) throw new Error("Skipped ClinVar observation contains request evidence");
    return null;
  }
  if (row.phase === "search" && !/^rs[1-9]\d*$/.test(row.rsid!)) throw new Error("Invalid staged ClinVar rsID");
  if (row.source_url !== clinvarUrl(row.phase, row.rsid, ids)) throw new Error("Staged ClinVar source URL mismatch");
  const native: unknown = row.native_response_json === null ? null : JSON.parse(row.native_response_json);
  if (row.native_response_json !== null && await hashCanonicalJson(native) !== row.response_sha256) throw new Error("Staged ClinVar native hash mismatch");
  if (row.status === "observed" && (native === null || row.http_status !== 200)) throw new Error("Observed ClinVar response lacks successful native evidence");
  return native;
}

/** Rebuild anchor context from native responses; do not trust staged display projections. */
export async function loadClinvarContext(namespace: DurableObjectNamespace, id: string, capsJson?: string | null, wasCollected = true) {
  const context = new Map<string, Record<string, unknown>[]>(); const caps = JSON.parse(capsJson ?? "{}");
  if (caps.include_clinvar !== true || !wasCollected || caps.clinvar_context_version === undefined) return context;
  if (caps.clinvar_context_version !== CLINVAR_CONTEXT_VERSION) throw new Error("Unrecognized staged ClinVar context version");
  const observations = await readCompleteTable<ClinvarObservation>(namespace, id, "clinvar_evidence");
  if (observations.length > 31) throw new Error("ClinVar observations exceed request bounds");
  const native = await Promise.all(observations.map(validateObservation));
  const summaries = observations.filter(row => row.phase === "summary");
  const searches = observations.filter(row => row.phase === "search");
  if (summaries.length > 1 || searches.length > 30 || new Set(searches.map(row => row.rsid)).size !== searches.length) throw new Error("Duplicate or excess staged ClinVar requests");
  return projectClinvarSearches(summaries, native, observations, searches, context, id);
}

function projectClinvarSearches(summaries: ClinvarObservation[], native: unknown[], observations: ClinvarObservation[], searches: ClinvarObservation[], context: Map<string, Record<string, unknown>[]>, id: string) {
  const summary = summaries[0]; const summaryNative = summary ? native[observations.indexOf(summary)] : null;
  const expectedIds = [...new Set(searches.filter(row => row.status === "observed").flatMap(row => searchIds(native[observations.indexOf(row)])))];
  if (summary && JSON.stringify(JSON.parse(summary.requested_ids_json)) !== JSON.stringify(expectedIds)) throw new Error("ClinVar summary does not match the searched IDs");
  for (const row of searches) {
    const { native_response_json: _raw, ...reference } = row;
    const rows = []; const errors: string[] = [];
    if (row.status === "observed") for (const uid of searchIds(native[observations.indexOf(row)])) {
      if (summary?.status !== "observed") { errors.push(`Summary unavailable for ClinVar UID ${uid}`); continue; }
      try { rows.push(clinvarProjection(row.rsid!, uid, summaryNative, row.response_sha256, summary.response_sha256)); }
      catch (error) { errors.push(String(error)); }
    }
    const summaryReference = summary ? (({ native_response_json: _raw, ...ref }) => ref)(summary) : null;
    context.set(row.rsid!, [{
      ...reference, context_only: true, coverage_complete: false, release_pinned: false,
      source_data_access_id: id, source_table: "clinvar_evidence", summary: summaryReference, records: rows, projection_errors: errors
    }]);
  }
  return context;
}
