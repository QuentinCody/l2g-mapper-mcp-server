import type { ColocRow, L2gPredictionRow, OtColoc } from "./opentargets-contract";
import { l2gGeneKey, qualifiedL2gScores, releaseKey } from "./qualified-l2g";
import { qualifyPredictionTrait, traitId } from "./opentargets-traits";

const molecularTypes = new Set(["eqtl", "pqtl", "sqtl", "tuqtl", "sceqtl", "scpqtl", "scsqtl", "sctuqtl"]);
export interface ColocSupport {
  method: string; metric: "h4" | "clpp"; value: number; study_locus_id: string;
  other_study_locus_id: string; other_study_id: string; ensembl_id: string;
  right_study_type: string; biosample_id: string | null; is_trans_qtl: boolean | null;
  requested_trait_id: string; data_version_json: string; lead_variant_id: string;
}
export interface ColocScores { h4: ColocSupport | null; clpp: ColocSupport | null }

function metric(native: OtColoc): { metric: "h4" | "clpp"; value: number } | null {
  const method = native.colocalisationMethod?.toUpperCase();
  const key = method === "COLOC" || method === "COLOC_PIP" ? "h4" : method === "ECAVIAR" ? "clpp" : null;
  const value = key ? native[key] : null;
  return key && typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 1 ? { metric: key, value } : null;
}

function qualify(row: ColocRow, selected: string | null): ColocSupport | null {
  const trait = qualifyPredictionTrait(row, selected);
  if (!selected || row.requested_trait_id !== selected || row.trait_match !== "exact_mapped_trait_id" || trait.trait_match !== "exact_mapped_trait_id" ||
      !row.study_locus_id || !row.lead_variant_id || !row.ensembl_id || row.genome_assembly !== "GRCh38" || row.anchor_match !== "exact_resolved_variant_id") return null;
  try {
    const left = JSON.parse(row.credible_set_json);
    const native = JSON.parse(row.native_coloc_json) as OtColoc;
    const right = native.otherStudyLocus; const study = right?.study; const score = metric(native);
    if (left?.studyLocusId !== row.study_locus_id || left?.variant?.id !== row.lead_variant_id || !score ||
        !right?.studyLocusId || right.studyLocusId === row.study_locus_id || !right.studyId || right.studyId === left.studyId || right.studyId !== study?.id || !native.rightStudyType ||
        !molecularTypes.has(native.rightStudyType) || right.studyType !== native.rightStudyType || study.studyType !== native.rightStudyType ||
        right.qtlGeneId !== row.ensembl_id || study.target?.id !== row.ensembl_id || row.other_study_id !== right.studyId ||
        row.method !== native.colocalisationMethod || row[score.metric] !== score.value) return null;
    return { ...score, method: native.colocalisationMethod!, study_locus_id: row.study_locus_id,
      other_study_locus_id: right.studyLocusId, other_study_id: right.studyId, ensembl_id: row.ensembl_id,
      right_study_type: native.rightStudyType, biosample_id: study.biosample?.biosampleId ?? null,
      is_trans_qtl: typeof right.isTransQtl === "boolean" ? right.isTransQtl : null,
      requested_trait_id: selected, data_version_json: row.data_version_json, lead_variant_id: row.lead_variant_id };
  } catch { return null; }
}

/** Candidate evidence must concern the same GWAS credible set, allele and release. */
export function qualifiedColocScores(rows: ColocRow[], predictions: L2gPredictionRow[], expectedTrait: string | null) {
  const eligible = qualifiedL2gScores(predictions, expectedTrait).eligibleRows;
  const keys = new Set(eligible.map(row => JSON.stringify([l2gGeneKey(row), row.study_locus_id, row.variant_id, releaseKey(row.data_version_json)])));
  const scores = new Map<string, ColocScores>(); let excluded = 0;
  for (const row of rows) {
    const support = qualify(row, traitId(expectedTrait)); const release = releaseKey(row.data_version_json);
    if (!support || !release || !row.lead_rsid || !row.locus_id) { excluded++; continue; }
    const identity = JSON.stringify([l2gGeneKey(row), row.study_locus_id, row.lead_variant_id, release]);
    if (!keys.has(identity)) { excluded++; continue; }
    const key = l2gGeneKey(row); const current = scores.get(key) ?? { h4: null, clpp: null };
    const previous = current[support.metric];
    const tie = (value: ColocSupport) => JSON.stringify([value.study_locus_id, value.other_study_locus_id, value.method]);
    if (!previous || support.value > previous.value || (support.value === previous.value && tie(support) < tie(previous))) current[support.metric] = support;
    scores.set(key, current);
  }
  return { scores, excluded };
}
