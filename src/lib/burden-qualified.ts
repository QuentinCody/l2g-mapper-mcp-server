import { hashCanonicalJson } from "@bio-mcp/shared";
import { OT_BURDEN_QUERY } from "./burden-evidence";
import { traitId } from "./opentargets-traits";

type Row = Record<string, any>;
const record = (value: unknown): value is Row => value !== null && typeof value === "object" && !Array.isArray(value);
const endpoint = "https://api.platform.opentargets.org/api/v4/graphql";
const text = (value: unknown): value is string => typeof value === "string" && value.trim().length > 0;

async function verifiedObservation(row: Row) {
  if ((row.usable !== true && row.usable !== 1) || row.error !== null || row.source_url !== endpoint || !text(row.retrieved_at)) return null;
  try {
    const native = JSON.parse(row.response_json);
    const version = native?.data?.meta?.dataVersion;
    if (!record(native) || (native.errors != null && (!Array.isArray(native.errors) || native.errors.length)) ||
        !record(version) || !text(version.year) || !text(version.month) || await hashCanonicalJson(native) !== row.response_sha256 ||
        await hashCanonicalJson(version) !== await hashCanonicalJson(JSON.parse(row.data_version_json))) return null;
    return { row, native, version, key: JSON.stringify([version.year, version.month, version.iteration ?? null]) };
  } catch { return null; }
}

function qualifiedP(row: Row, genes: Set<string>, trait: string): number | null {
  if (!text(row.id) || row.datasourceId !== "gene_burden" || row.datatypeId !== "genetic_association" ||
      !genes.has(row.target?.id) || row.disease?.id !== trait || row.diseaseFromSourceMappedId !== trait ||
      !text(row.studyId) || !text(row.statisticalMethod) || !Array.isArray(row.qualityControls) || row.qualityControls.length ||
      (row.warningMessage !== null && row.warningMessage !== undefined && row.warningMessage !== "")) return null;
  const { pValueMantissa: mantissa, pValueExponent: exponent } = row;
  if (typeof mantissa !== "number" || !Number.isFinite(mantissa) || mantissa <= 0 || !Number.isSafeInteger(exponent)) return null;
  const p = mantissa * 10 ** exponent;
  return Number.isFinite(p) && p > 0 && p <= 1 ? p : null;
}

/** Stored native evidence is rechecked at scoring time, not trusted from a derived minimum. */
export async function qualifiedBurden(observations: Row[], selectedTrait: string | null, dataAccessId: string) {
  const support = new Map<string, { p_value: number; references: Row[] }>();
  const warnings: string[] = [];
  const selected = traitId(selectedTrait);
  const burden = observations.filter(row => row.phase === "gene burden");
  if (!selected || !burden.length) return { support, warnings };
  const pages = await Promise.all(observations.filter(row => row.usable === true || row.usable === 1).map(verifiedObservation));
  if (pages.some(page => !page) || new Set(pages.map(page => page!.key)).size !== 1) {
    warnings.push("Open Targets burden excluded: native response integrity or consistent data-release evidence is missing.");
    return { support, warnings };
  }
  let { entries, excluded } = collectBurdenEntries(pages, selected, dataAccessId);
  excluded = mergeBurdenEntries(entries, excluded, support);
  if (excluded) warnings.push(`${excluded} burden rows/pages lack exact gene/trait/source, valid p-value, clean QC or consistent native evidence and were excluded. Missing support is not evidence of absence.`);
  return { support, warnings };
}

function mergeBurdenEntries(entries: { gene: string; p: number; identity: string; native: string; reference: Row; }[], previouslyExcluded: number, support: Map<string, { p_value: number; references: Row[]; }>) {
  let excluded = previouslyExcluded;
  const seen = new Map<string, Set<string>>();
  for (const entry of entries) { const values = seen.get(entry.identity) ?? new Set(); values.add(entry.native); seen.set(entry.identity, values); }
  entries.sort((a, b) => a.identity.localeCompare(b.identity));
  for (const entry of entries) {
    if (seen.get(entry.identity)!.size !== 1) { excluded++; continue; }
    const current = support.get(entry.gene) ?? { p_value: entry.p, references: [] };
    if (!current.references.some(ref => ref.evidence_id === entry.identity)) current.references.push(entry.reference);
    current.p_value = Math.min(current.p_value, entry.p); support.set(entry.gene, current);
  }
  return excluded;
}

function collectBurdenEntries(pages: Awaited<ReturnType<typeof verifiedObservation>>[], selected: string, dataAccessId: string) {
  const entries: Array<{ gene: string; p: number; identity: string; native: string; reference: Row }> = [];
  let excluded = 0;
  for (const page of pages) {
    if (!page || page.row.phase !== "gene burden") continue;
    let request: Row;
    try { request = JSON.parse(page.row.variables_json); } catch { excluded++; continue; }
    const disease = page.native.data.disease;
    if (page.row.query !== OT_BURDEN_QUERY || !record(request) || request.trait !== selected || !Array.isArray(request.genes) ||
        !request.genes.length || request.genes.length > 25 || request.genes.some((id: unknown) => typeof id !== "string" || !/^ENSG\d+$/.test(id)) ||
        disease?.id !== selected || !Array.isArray(disease?.evidences?.rows) || disease.evidences.rows.length > 300 ||
        !Number.isSafeInteger(disease.evidences.count) || disease.evidences.count < disease.evidences.rows.length ||
        (disease.evidences.cursor != null && typeof disease.evidences.cursor !== "string") ||
        !Array.isArray(page.native.data.associationDatasources) || !page.native.data.associationDatasources.some((s: Row) => s?.datasource === "gene_burden" && s.datatype === "genetic_association")) { excluded++; continue; }
    const genes = new Set<string>(request.genes);
    for (const [index, native] of disease.evidences.rows.entries()) {
      const p = record(native) ? qualifiedP(native, genes, selected) : null;
      if (p === null) { excluded++; continue; }
      entries.push({ gene: native.target.id, p, identity: native.id, native: JSON.stringify(native), reference: {
        source_data_access_id: dataAccessId, source_table: "opentargets_evidence", source_response_sha256: page.row.response_sha256,
        source_row_index: index, source_url: endpoint, retrieved_at: page.row.retrieved_at, data_version: page.version,
        evidence_id: native.id, native_evidence: native, requested_trait_id: selected,
        coverage_complete: false, release_pinned: false, locus_specific: false,
      } });
    }
  }
  return { entries, excluded };
}
