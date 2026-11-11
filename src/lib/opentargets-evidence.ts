import { createOpenTargetsObservations, type OpenTargetsEvidenceRow } from "./opentargets-observations";
import { OT_SEARCH_STUDY_QUERY, OT_STUDY_CS_QUERY, OT_BATCH_DETAIL_QUERY, type OtStudy, type OtSearchResp, type OtStudyResp, type OtBatchResp, type L2gPredictionRow, type ColocRow } from "./opentargets-contract";
import { appendOpenTargetsDetails } from "./opentargets-projection";
import { indexOpenTargetsAnchors, matchOpenTargetsLoci, type OpenTargetsAnchor } from "./opentargets-matching";
import { qualifyBundleTraits, qualifyPredictionTrait, studyMatchesTrait, traitId } from "./opentargets-traits";
export type { OpenTargetsEvidenceRow } from "./opentargets-observations";
export type { L2gPredictionRow, ColocRow } from "./opentargets-contract";
type Observations = ReturnType<typeof createOpenTargetsObservations>;

async function findStudies(observations: Observations, terms: string[], warnings: string[], selected: string | null) {
  const results = await Promise.all(terms.map(q => observations.request<OtSearchResp>("study search", OT_SEARCH_STUDY_QUERY, { q })));
  const studies = new Map<string, OtStudy>();
  for (const result of results) {
    const hits = result.data?.search?.hits ?? [];
    if ((result.data?.search?.total ?? hits.length) > hits.length) warnings.push("OpenTargets study search truncated at 20 hits; discovery is incomplete");
    for (const hit of hits) if (hit.object?.id) studies.set(hit.object.id, hit.object);
  }
  const chosen = [...studies.values()].sort((a, b) => Number(studyMatchesTrait(b, selected)) - Number(studyMatchesTrait(a, selected)) || Number(!!b.hasSumstats) - Number(!!a.hasSumstats) || a.id.localeCompare(b.id)).slice(0, 6);
  if (chosen.length < studies.size) warnings.push("OpenTargets selects at most six studies, preferring exact mapped GWAS traits then summary statistics; this is not an exhaustive study review");
  return chosen;
}

async function matchAnchors(observations: Observations, studies: OtStudy[], index: Map<string, Set<string>>, warnings: string[]) {
  const results = await Promise.all(studies.map(s => observations.request<OtStudyResp>("study credible sets", OT_STUDY_CS_QUERY, { id: s.id })));
  const allRows = [];
  for (const result of results) {
    const sets = result.data?.study?.credibleSets;
    const rows = sets?.rows ?? [];
    if ((sets?.count ?? rows.length) > rows.length) warnings.push("OpenTargets study credible sets truncated at 400 rows; anchor absence is not established");
    allRows.push(...rows);
  }
  return matchOpenTargetsLoci(allRows, index, warnings);
}

export interface OpenTargetsBundle {
  l2g: L2gPredictionRow[]; coloc: ColocRow[];
  symbolsNeedingEnsembl: Map<string, string>; evidence: OpenTargetsEvidenceRow[];
}
export async function fetchOpenTargetsBundle(anchors: OpenTargetsAnchor[], traitTerms: string[], maxColocRows: number, warnings: string[], selectedTrait: string | null = null): Promise<OpenTargetsBundle> {
  const observations = createOpenTargetsObservations(warnings);
  const result: OpenTargetsBundle = { l2g: [], coloc: [], symbolsNeedingEnsembl: new Map(), evidence: observations.evidence };
  const index = indexOpenTargetsAnchors(anchors, warnings);
  if (!index.size || !traitTerms.length) return result;
  const studies = await findStudies(observations, traitTerms, warnings, traitId(selectedTrait));
  const matches = await matchAnchors(observations, studies, index, warnings);
  const ids = [...matches.keys()].slice(0, 200);
  if (matches.size > ids.length) warnings.push("OpenTargets detail gathering capped at 200 study loci; only the selected subset is ranked");
  const requests = [];
  for (let index = 0; index < ids.length; index += 40) {
    const chunk = ids.slice(index, index + 40);
    requests.push(observations.request<OtBatchResp>("credible-set detail", OT_BATCH_DETAIL_QUERY, { ids: chunk, size: chunk.length, colocSize: maxColocRows }));
  }
  const responses = await Promise.all(requests);
  if (!observations.consistent()) return result;
  for (const response of responses) {
    const sets = response.data?.credibleSets;
    if (sets && (sets.count ?? sets.rows?.length ?? 0) > (sets.rows?.length ?? 0)) warnings.push("OpenTargets credible-set detail response is incomplete; inspect native count");
  }
  appendOpenTargetsDetails({ meta: responses.find(r => r.data)?.data?.meta, credibleSets: { rows: responses.flatMap(r => r.data?.credibleSets?.rows ?? []) } }, matches, result, maxColocRows, warnings);
  qualifyBundleTraits(result.l2g, traitId(selectedTrait), warnings);
  for (const row of result.coloc) Object.assign(row, qualifyPredictionTrait(row, selectedTrait));
  return result;
}

export function applyOpenTargetsBundle(result: OpenTargetsBundle, anchors: Array<{ rsid: string; locus_id?: string }>, tables: { l2g_predictions: L2gPredictionRow[]; coloc: ColocRow[]; opentargets_evidence: OpenTargetsEvidenceRow[] }, symbols: Map<string, string>) {
  for (const row of [...result.l2g, ...result.coloc]) row.locus_id = anchors.find(a => a.rsid === row.lead_rsid)?.locus_id ?? row.locus_id;
  tables.l2g_predictions = result.l2g;
  tables.coloc = result.coloc;
  tables.opentargets_evidence = result.evidence;
  for (const [symbol, id] of result.symbolsNeedingEnsembl) symbols.set(symbol, id);
}
