import { observeClinvar, searchIds, object, type ClinvarObservation } from "./clinvar-observation";

export interface ClinvarRow {
  rsid: string; variation_id: string; title: string | null; clinical_significance: string | null; review_status: string | null;
  clinical_significance_basis: "germline_classification" | "legacy_clinical_significance" | null;
  somatic_clinical_impact: string | null; oncogenicity: string | null; accession_version: string | null;
  search_response_sha256: string | null; summary_response_sha256: string | null;
  context_only: true; exact_allele_match: false; trait_match: false;
}
const text = (value: unknown) => typeof value === "string" ? value : null;
export function clinvarProjection(rsid: string, id: string, native: unknown, searchHash: string | null, summaryHash: string | null): ClinvarRow {
  if (!object(native) || native.error || !object(native.result)) throw new Error("Unrecognized ClinVar summary response");
  const rec = native.result[id];
  if (!Array.isArray(native.result.uids) || !native.result.uids.includes(id) || !object(rec) || rec.uid !== id || rec.error) throw new Error(`ClinVar summary missing/mismatched UID ${id}`);
  const germline = object(rec.germline_classification) ? rec.germline_classification : null;
  const legacy = object(rec.clinical_significance) ? rec.clinical_significance : null;
  return { rsid, variation_id: id, title: text(rec.title), clinical_significance: text((germline ?? legacy)?.description),
    review_status: text(germline?.review_status ?? rec.review_status),
    clinical_significance_basis: germline ? "germline_classification" : legacy ? "legacy_clinical_significance" : null,
    somatic_clinical_impact: object(rec.clinical_impact_classification) ? text(rec.clinical_impact_classification.description) : null,
    oncogenicity: object(rec.oncogenicity_classification) ? text(rec.oncogenicity_classification.description) : null,
    accession_version: text(rec.accession_version), search_response_sha256: searchHash, summary_response_sha256: summaryHash,
    context_only: true, exact_allele_match: false, trait_match: false };
}

/** Native search and batch-summary observations remain separate to avoid duplicating large payloads. */
export async function fetchClinvarLane(rsids: string[], warnings: string[]) {
  const evidence: ClinvarObservation[] = []; const rows: ClinvarRow[] = [];
  const selected = [...new Set(rsids)].slice(0, 30); const deadline = Date.now() + 13_000;
  const searches: { row: ClinvarObservation; ids: string[] }[] = [];
  if (new Set(rsids).size > 30) warnings.push("ClinVar capped at 30 distinct rsIDs; coverage is incomplete");
  for (const rsid of selected) {
    const skip = !/^rs[1-9]\d*$/.test(rsid) ? "Invalid rsID" : Date.now() + 8500 >= deadline ? "ClinVar lane deadline; search not attempted" : undefined;
    const row = await observeClinvar("search", rsid, [], skip); evidence.push(row);
    if (row.status === "observed") {
      try { searches.push({ row, ids: searchIds(JSON.parse(row.native_response_json!)) }); }
      catch (error) { row.status = "error"; row.error = String(error); }
    }
    // Keep this lane below the documented unauthenticated three-requests/second limit.
    if (!skip) await new Promise(resolve => setTimeout(resolve, 350));
  }
  const ids = [...new Set(searches.flatMap(search => search.ids))];
  if (ids.length) {
    const summary = await observeClinvar("summary", null, ids); evidence.push(summary);
    if (summary.status === "observed") {
      const native: unknown = JSON.parse(summary.native_response_json!);
      for (const search of searches) for (const id of search.ids) {
        try { rows.push(clinvarProjection(search.row.rsid!, id, native, search.row.response_sha256, summary.response_sha256)); }
        catch (error) { warnings.push(String(error)); }
      }
      if (!object(native) || native.error || !object(native.result)) { summary.status = "error"; summary.error = "Unrecognized ClinVar summary response"; }
    }
  }
  for (const row of evidence) if (row.error) warnings.push(`ClinVar ${row.phase} ${row.rsid ?? "batch"}: ${row.error}`);
  warnings.push("ClinVar searches retain at most three IDs per rsID. Search membership is related-variant context, not an exact allele, gene or trait match; it does not affect L2G scores. Native responses are in clinvar_evidence; release and coverage are not pinned/complete.");
  return { rows, evidence };
}
