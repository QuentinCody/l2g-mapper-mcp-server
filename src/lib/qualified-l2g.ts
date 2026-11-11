import type { L2gPredictionRow } from "./opentargets-contract";
import { qualifyPredictionTrait, traitId } from "./opentargets-traits";

interface GeneKey { locus_id: string; lead_rsid: string; ensembl_id: string | null }
export interface L2gSupport {
  score: number; study_locus_id: string; variant_id: string;
  ensembl_id: string; data_version_json: string;
  genome_assembly: "GRCh38"; anchor_match: "exact_resolved_variant_id";
  model_version: null;
  requested_trait_id: string; trait_match: "exact_mapped_trait_id";
}

export function l2gGeneKey(row: GeneKey): string {
  return JSON.stringify([row.locus_id, row.lead_rsid.toLowerCase(), row.ensembl_id]);
}

export function releaseKey(value: string): string | null {
  try {
    const version = JSON.parse(value);
    if (typeof version?.year !== "string" || !version.year || typeof version.month !== "string" || !version.month) return null;
    return JSON.stringify([version.year, version.month, version.iteration ?? null]);
  } catch { return null; }
}

/** Gene symbols are display labels; do not transfer evidence between stable IDs or anchors. */
export function qualifiedL2gScores(rows: L2gPredictionRow[], expectedTrait?: string | null) {
  const scores = new Map<string, L2gSupport>(); const releases = new Set<string>(); const traits = new Set<string>();
  const eligibleRows: L2gPredictionRow[] = [];
  let excluded = 0;
  for (const row of rows) {
    const release = releaseKey(row.data_version_json);
    const trait = qualifyPredictionTrait(row, row.requested_trait_id);
    if (!row.ensembl_id || !row.lead_rsid || !row.locus_id || !row.study_locus_id || !row.variant_id ||
      row.trait_match !== "exact_mapped_trait_id" || trait.trait_match !== "exact_mapped_trait_id" || !trait.requested_trait_id ||
      (expectedTrait !== undefined && trait.requested_trait_id !== traitId(expectedTrait)) ||
      row.genome_assembly !== "GRCh38" || row.anchor_match !== "exact_resolved_variant_id" || !release ||
      typeof row.score !== "number" || !Number.isFinite(row.score) || row.score < 0 || row.score > 1) {
      excluded++; continue;
    }
    releases.add(release);
    traits.add(trait.requested_trait_id);
    eligibleRows.push(row);
    const key = l2gGeneKey(row); const previous = scores.get(key);
    // A deterministic representative of tied maxima; not a count of independent studies.
    if (previous && (previous.score > row.score || (previous.score === row.score && previous.study_locus_id <= row.study_locus_id))) continue;
    scores.set(key, {
      score: row.score, study_locus_id: row.study_locus_id, variant_id: row.variant_id,
      ensembl_id: row.ensembl_id, data_version_json: row.data_version_json,
      genome_assembly: row.genome_assembly, anchor_match: row.anchor_match, model_version: null,
      requested_trait_id: trait.requested_trait_id, trait_match: "exact_mapped_trait_id",
    });
  }
  if (releases.size > 1) throw new Error("Stored Open Targets predictions span data releases. Re-run l2g_gather before scoring.");
  if (traits.size > 1) throw new Error("Stored Open Targets predictions span selected traits. Re-run l2g_gather before scoring.");
  return { scores, excluded, eligibleRows };
}
