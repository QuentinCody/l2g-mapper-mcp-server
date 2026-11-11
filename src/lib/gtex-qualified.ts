import { hashCanonicalJson } from "@bio-mcp/shared";
import type { EqtlRow } from "./gtex-evidence";

type Row = Record<string, unknown>;
const record = (value: unknown): Row => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Row : {};
const numeric = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? value : null;
export const gtexGeneKey = (row: { lead_rsid: string; ensembl_id: string | null }) => JSON.stringify([row.lead_rsid.toLowerCase(), row.ensembl_id]);
const ENDPOINT = "https://gtexportal.org/api/v2/association/singleTissueEqtl";

interface Page { observation: Row; data: unknown[]; variants: string[] }

async function qualifyPage(observation: Row, dataset: unknown): Promise<Page | null> {
  if ((dataset !== "gtex_v8" && dataset !== "gtex_v10") || observation.dataset_id_requested !== dataset ||
      (observation.usable !== true && observation.usable !== 1) || observation.http_status !== 200 || observation.error !== null) return null;
  try {
    const response = record(JSON.parse(String(observation.response_json)));
    const request = record(JSON.parse(String(observation.request_json)));
    if (!Array.isArray(response.data) || (response.datasetId !== undefined && response.datasetId !== dataset) ||
        await hashCanonicalJson(response) !== observation.response_sha256 || request.datasetId !== dataset ||
        !Array.isArray(request.variantId) || !request.variantId.length || request.variantId.some(id => typeof id !== "string")) return null;
    const url = new URL(ENDPOINT);
    for (const [key, value] of Object.entries(request)) {
      for (const item of Array.isArray(value) ? value : [value]) url.searchParams.append(key, String(item));
    }
    if (url.href !== observation.source_url || typeof observation.retrieved_at !== "string") return null;
    return { observation, data: response.data, variants: request.variantId as string[] };
  } catch { return null; }
}

function anchorMatches(anchors: Row[], row: EqtlRow) {
  const matches = anchors.filter(anchor => typeof anchor.rsid === "string" && anchor.rsid.toLowerCase() === row.lead_rsid.toLowerCase());
  if (!matches.length) return false;
  return matches.every(anchor => Number.isSafeInteger(anchor.grch38_pos) && Number(anchor.grch38_pos) > 0 &&
    [anchor.grch38_ref, anchor.grch38_alt].every(allele => typeof allele === "string" && /^[ACGT]+$/.test(allele)) &&
    row.variant_id === `chr${anchor.grch38_chr}_${anchor.grch38_pos}_${anchor.grch38_ref}_${anchor.grch38_alt}_b38`);
}

function nativeMatches(row: EqtlRow, page: Page) {
  const native = record(page.data[row.source_row_index]);
  return Number.isSafeInteger(row.source_row_index) && row.source_row_index >= 0 &&
    typeof native.gencodeId === "string" && native.gencodeId === row.gencode_id && native.gencodeId.split(".")[0] === row.ensembl_id &&
    native.variantId === row.variant_id && native.tissueSiteDetailId === row.tissue && native.geneSymbol === row.symbol &&
    (native.datasetId === undefined || native.datasetId === row.dataset_id_requested) && page.variants.includes(row.variant_id) &&
    JSON.stringify(native) === row.native_row_json && numeric(native.pValue) === row.p_value && numeric(native.nes) === row.slope;
}

function reference(row: EqtlRow, page: Page, dataAccessId: string) {
  return { source_data_access_id: dataAccessId, source_table: "gtex_evidence", projection_table: "eqtl",
    response_sha256: row.source_response_sha256, source_row_index: row.source_row_index,
    source_url: page.observation.source_url, retrieved_at: page.observation.retrieved_at,
    lead_rsid: row.lead_rsid, ensembl_id: row.ensembl_id, gencode_id: row.gencode_id,
    variant_id: row.variant_id, genome_assembly: row.genome_assembly,
    dataset_id_requested: row.dataset_id_requested, tissue: row.tissue, p_value: row.p_value,
    tissue_ontology_id: record(page.data[row.source_row_index]).ontologyId ?? null,
    normalized_effect_size: row.slope, coverage_complete: false, release_pinned: false };
}

/** Recheck native page/row integrity, requested dataset and resolved anchor before scoring. */
export async function qualifiedGtex(rows: EqtlRow[], observations: Row[], anchors: Row[], dataset: unknown, dataAccessId: string) {
  const pages: Page[] = [];
  for (const observation of observations) {
    const page = await qualifyPage(observation, dataset); if (page) pages.push(page);
  }
  pages.sort((a, b) => String(a.observation.source_url).localeCompare(String(b.observation.source_url)));
  const support = new Map<string, { tissues: Set<string>; references: ReturnType<typeof reference>[] }>();
  const seen = new Set<string>(); let excluded = 0;
  for (const row of rows) {
    if (typeof row.lead_rsid !== "string" || !row.ensembl_id || !row.tissue || row.tissue === "." ||
        row.genome_assembly !== "GRCh38" || row.dataset_id_requested !== dataset || !anchorMatches(anchors, row)) { excluded++; continue; }
    const page = pages.find(value => value.observation.response_sha256 === row.source_response_sha256 && nativeMatches(row, value));
    if (!page) { excluded++; continue; }
    const key = gtexGeneKey(row); const identity = JSON.stringify([key, row.source_response_sha256, row.source_row_index]);
    if (seen.has(identity)) continue;
    seen.add(identity);
    const current = support.get(key) ?? { tissues: new Set<string>(), references: [] };
    current.tissues.add(row.tissue); current.references.push(reference(row, page, dataAccessId)); support.set(key, current);
  }
  for (const current of support.values()) current.references.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
  return { support, warnings: excluded ? [`${excluded} GTEx rows lack matching native page, dataset, gene, tissue or anchor evidence and were excluded from scoring. Re-gather legacy inputs.`] : [] };
}
