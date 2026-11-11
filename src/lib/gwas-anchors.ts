/** Project a bounded GWAS discovery page; these anchors are not qualified gene evidence. */
export interface GwasAnchor {
  rsid: string;
  source: "gwas";
  p_value: number | null;
  lead_trait: string | null;
  cohort: null;
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

function associationRows(data: unknown): unknown[] {
  const root = record(data);
  if (!root || root.error || root.errors) throw new Error("GWAS response is not an association page");
  const embedded = record(root._embedded);
  const content = root.content;
  const hal = embedded?.associations;
  if (content !== undefined && hal !== undefined) throw new Error("GWAS response has ambiguous association collections");
  const rows = content ?? hal;
  if (Array.isArray(rows)) return rows;
  if (content === undefined && hal === undefined && record(root.page)?.totalElements === 0) return [];
  throw new Error("GWAS response is missing a valid association collection");
}

function singleRsid(row: Record<string, unknown>): string | null {
  if (row.multi_snp_haplotype === true || row.snp_interaction === true) return null;
  const values: unknown[] = [];
  if (row.rs_id !== undefined && row.rs_id !== null) values.push(row.rs_id);
  if (row.snp_allele !== undefined && row.snp_allele !== null) {
    if (!Array.isArray(row.snp_allele)) return null;
    values.push(...row.snp_allele.map(allele => record(allele)?.rs_id));
  }
  if (!values.length || values.some(value => typeof value !== "string" || !/^rs\d+$/i.test(value))) return null;
  const ids = new Set(values.map(value => (value as string).toLowerCase()));
  return ids.size === 1 ? [...ids][0] : null;
}

function traitLabel(row: Record<string, unknown>): string | null {
  const mapped = Array.isArray(row.efo_traits) ? record(row.efo_traits[0])?.efo_trait : undefined;
  if (typeof mapped === "string" && mapped) return mapped;
  const reported = Array.isArray(row.reported_trait) ? row.reported_trait[0] : row.reported_trait;
  return typeof reported === "string" && reported ? reported : null;
}

export function projectGwasAnchors(data: unknown, limit: number, warnings: string[]): GwasAnchor[] {
  const rows = associationRows(data);
  const selected = new Map<string, GwasAnchor>();
  let skipped = 0;
  for (const value of rows.slice(0, limit)) {
    const row = record(value);
    const rsid = row && singleRsid(row);
    if (!row || !rsid) { skipped++; continue; }
    const p = row.p_value;
    const pValue = typeof p === "number" && Number.isFinite(p) && p >= 0 && p <= 1 ? p : null;
    const previous = selected.get(rsid);
    if (previous && (previous.p_value ?? Number.POSITIVE_INFINITY) <= (pValue ?? Number.POSITIVE_INFINITY)) continue;
    selected.set(rsid, { rsid, source: "gwas", p_value: pValue, lead_trait: traitLabel(row), cohort: null });
  }
  if (skipped) warnings.push(`GWAS Catalog skipped ${skipped} malformed, multi-SNP or interaction associations without a single unambiguous rsID`);
  if (rows.length > limit) warnings.push(`GWAS Catalog response exceeded the ${limit}-association cap; excess rows were not projected`);
  if (rows.length) warnings.push("GWAS Catalog anchors use one bounded page; retained p-values rank only that page, not the complete trait association set");
  return [...selected.values()];
}
