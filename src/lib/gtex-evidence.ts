import type { VariantCoord } from "@bio-mcp/shared/variants/resolve";
import { observeGtex, type GtexDataset, type GtexObservation } from "./gtex-observation";

export interface EqtlRow {
  lead_rsid: string;
  variant_id: string;
  genome_assembly: "GRCh38";
  dataset_id_requested: GtexDataset;
  source_response_sha256: string;
  source_row_index: number;
  native_row_json: string;
  gencode_id: string | null;
  symbol: string;
  ensembl_id: string | null;
  tissue: string;
  p_value: number | null;
  slope: number | null;
}

interface NativeEqtl {
  datasetId?: unknown;
  variantId?: string;
  geneSymbol?: string;
  gencodeId?: string;
  tissueSiteDetailId?: string;
  pValue?: number;
  nes?: number;
}

function projectRows(rows: unknown[], variants: Map<string, string>, dataset: GtexDataset, hash: string): EqtlRow[] {
  const out: EqtlRow[] = [];
  for (const [index, value] of rows.entries()) {
    if (!value || typeof value !== "object" || Array.isArray(value)) continue;
    const row = value as NativeEqtl;
    const variantId = row.variantId ?? "";
    // An rsID alone cannot establish assembly or allele compatibility.
    const rsid = variants.get(variantId);
    if (!rsid || typeof row.geneSymbol !== "string" || !row.geneSymbol ||
        (row.datasetId !== undefined && row.datasetId !== dataset)) continue;
    out.push({
      lead_rsid: rsid, variant_id: variantId, genome_assembly: "GRCh38", dataset_id_requested: dataset,
      source_response_sha256: hash, source_row_index: index, native_row_json: JSON.stringify(value),
      gencode_id: typeof row.gencodeId === "string" ? row.gencodeId : null, symbol: row.geneSymbol,
      ensembl_id: typeof row.gencodeId === "string" ? row.gencodeId.split(".")[0] : null,
      tissue: typeof row.tissueSiteDetailId === "string" ? row.tissueSiteDetailId : ".",
      p_value: typeof row.pValue === "number" && Number.isFinite(row.pValue) ? row.pValue : null,
      slope: typeof row.nes === "number" && Number.isFinite(row.nes) ? row.nes : null,
    });
  }
  return out;
}

export async function fetchGtexEqtls(
  anchors: Array<{ rsid: string; grch38?: VariantCoord | null }>,
  maxRowsPerVariant: number,
  warnings: string[],
  dataset: GtexDataset = "gtex_v8",
): Promise<{ rows: EqtlRow[]; evidence: GtexObservation[] }> {
  if (!["gtex_v8", "gtex_v10"].includes(dataset)) throw new Error("Unsupported GTEx dataset; use gtex_v8 or gtex_v10");
  const variants = new Map<string, string>();
  for (const anchor of anchors) {
    const coord = anchor.grch38;
    if (!coord?.chr || !coord.pos || !coord.ref || !coord.alt) continue;
    variants.set(`chr${coord.chr}_${coord.pos}_${coord.ref}_${coord.alt}_b38`, anchor.rsid);
  }
  if (!variants.size) return { rows: [], evidence: [] };
  const result = await observeGtex({ variantId: [...variants.keys()], datasetId: dataset,
    page: 0, itemsPerPage: Math.min(250, maxRowsPerVariant * variants.size) }, dataset);
  if (result.evidence.error) warnings.push(`GTEx eQTL batch failed: ${result.evidence.error}`);
  else warnings.push(`GTEx: one capped association page requested from ${dataset}; coverage and source snapshot are not verified. Inspect gtex_evidence for all native rows and pagination.`);
  return { rows: result.evidence.usable && result.evidence.response_sha256 ? projectRows(result.rows, variants, dataset, result.evidence.response_sha256) : [], evidence: [result.evidence] };
}
