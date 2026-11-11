import type { L2gPredictionRow, OtBatchResp, OtDetail, OtPrediction } from "./opentargets-contract";
import type { OpenTargetsBundle } from "./opentargets-evidence";
import type { OpenTargetsMatches } from "./opentargets-matching";

function predictionRow(pred: OtPrediction, row: OtDetail, anchor: string, release: unknown): L2gPredictionRow | null {
  const symbol = pred.target?.approvedSymbol;
  if (!symbol || typeof pred.score !== "number" || !Number.isFinite(pred.score) || pred.score < 0 || pred.score > 1) return null;
  return {
    locus_id: `rsid:${anchor}`, lead_rsid: anchor, study_locus_id: row.studyLocusId ?? null,
    symbol, ensembl_id: pred.target?.id ?? null, score: pred.score,
    data_version_json: JSON.stringify(release ?? null), model_version: null,
    features_json: JSON.stringify(pred.features ?? null), shap_base_value: pred.shapBaseValue ?? null,
    variant_id: row.variant?.id ?? null, study_json: JSON.stringify(row.study ?? null),
    genome_assembly: "GRCh38", anchor_match: "exact_resolved_variant_id",
    requested_trait_id: null, trait_match: "unresolved",
    credible_set_json: JSON.stringify({ ...row, l2GPredictions: undefined, colocalisation: undefined }),
  };
}

function appendLocus(row: OtDetail, anchor: string, release: unknown, result: OpenTargetsBundle, maxColoc: number) {
  for (const pred of row.l2GPredictions?.rows ?? []) {
    const normalized = predictionRow(pred, row, anchor, release);
    if (!normalized) continue;
    result.l2g.push(normalized);
    if (normalized.ensembl_id) result.symbolsNeedingEnsembl.set(normalized.symbol, normalized.ensembl_id);
  }
  for (const coloc of (row.colocalisation?.rows ?? []).slice(0, maxColoc)) {
    result.coloc.push({
      locus_id: `rsid:${anchor}`, lead_rsid: anchor, study_locus_id: row.studyLocusId ?? null,
      method: coloc.colocalisationMethod ?? null,
      h4: typeof coloc.h4 === "number" ? coloc.h4 : null,
      clpp: typeof coloc.clpp === "number" ? coloc.clpp : null,
      other_study_id: coloc.otherStudyLocus?.studyId ?? null,
      lead_variant_id: row.variant?.id ?? null, genome_assembly: "GRCh38", anchor_match: "exact_resolved_variant_id",
      data_version_json: JSON.stringify(release ?? null), study_json: JSON.stringify(row.study ?? null),
      credible_set_json: JSON.stringify({ ...row, l2GPredictions: undefined, colocalisation: undefined }),
      native_coloc_json: JSON.stringify(coloc), ensembl_id: coloc.otherStudyLocus?.qtlGeneId ?? null,
      requested_trait_id: null, trait_match: "unresolved",
    });
  }
}

function warnPages(row: OtDetail, maxColoc: number, warnings: string[]) {
  const predictions = row.l2GPredictions?.rows ?? [];
  const coloc = row.colocalisation?.rows ?? [];
  if ((row.l2GPredictions?.count ?? predictions.length) > predictions.length) warnings.push(`OpenTargets ${row.studyLocusId}: L2G predictions truncated at 100 rows`);
  if ((row.colocalisation?.count ?? coloc.length) > Math.min(coloc.length, maxColoc)) warnings.push(`OpenTargets ${row.studyLocusId}: colocalisation page is incomplete`);
}

function incompatibleDetails(rows: OtDetail[], matches: OpenTargetsMatches, warnings: string[]) {
  const incompatible = new Set<string>();
  for (const row of rows) {
    const id = row.studyLocusId;
    if (id && matches.has(id) && row.variant?.id !== matches.get(id)!.variantId) incompatible.add(id);
  }
  for (const id of incompatible) warnings.push(`OpenTargets ${id}: detail lead variant differs from qualified discovery; locus excluded`);
  return incompatible;
}

export function appendOpenTargetsDetails(data: OtBatchResp, matches: OpenTargetsMatches, result: OpenTargetsBundle, maxColoc: number, warnings: string[]) {
  const rows = data.credibleSets?.rows ?? [];
  const incompatible = incompatibleDetails(rows, matches, warnings);
  if ((data.credibleSets?.count ?? rows.length) > rows.length) warnings.push("OpenTargets credible-set detail response is incomplete; inspect native count");
  for (const row of rows) {
    if (!row.studyLocusId || incompatible.has(row.studyLocusId)) continue;
    warnPages(row, maxColoc, warnings);
    for (const anchor of matches.get(row.studyLocusId)?.rsids ?? []) appendLocus(row, anchor, data.meta?.dataVersion, result, maxColoc);
  }
}
