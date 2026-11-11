import { createOpenTargetsObservations } from "./opentargets-observations";
import { OT_META } from "./opentargets-observations";
import { traitId } from "./opentargets-traits";

/** Provider SDL checked 2026-10-01: disease.evidences supports exact, non-propagated pairs.
 * https://api.platform.opentargets.org/api/v4/graphql/schema */
export const OT_BURDEN_QUERY = `query L2GBurden($genes: [String!]!, $trait: String!) {
  ${OT_META}
  associationDatasources { datasource datatype }
  disease(efoId: $trait) { id name
    evidences(ensemblIds: $genes, datasourceIds: ["gene_burden"], enableIndirect: false, size: 300) {
      count cursor rows { id datasourceId datatypeId
        target { id approvedSymbol } disease { id name }
        diseaseFromSource diseaseFromSourceId diseaseFromSourceMappedId
        studyId studyOverview projectId cohortId cohortDescription ancestry ancestryId
        statisticalMethod studySampleSize studyCases studyCasesWithQualifyingVariants
        pValueMantissa pValueExponent beta betaConfidenceIntervalLower betaConfidenceIntervalUpper
        oddsRatio oddsRatioConfidenceIntervalLower oddsRatioConfidenceIntervalUpper
        allelicRequirements literature qualityControls warningMessage confidence releaseVersion releaseDate
      }
    }
  }
}`;

/** One bounded page across selected candidate genes; never infer complete source coverage. */
export async function fetchBurdenEvidence(candidates: Array<{ ensembl_id: string | null }>, selectedTrait: string | null, warnings: string[]) {
  const observations = createOpenTargetsObservations(warnings);
  const trait = traitId(selectedTrait);
  const allGenes = [...new Set(candidates.map(row => row.ensembl_id).filter((id): id is string => typeof id === "string" && /^ENSG\d+$/.test(id)))].sort();
  if (!trait || !allGenes.length) return observations.evidence;
  const genes = allGenes.slice(0, 25);
  if (genes.length < allGenes.length) warnings.push("Open Targets gene-burden evidence is limited to 25 selected candidate genes; unqueried genes have no burden contribution.");
  const result = await observations.request<{ disease?: { evidences?: { count?: number; cursor?: string; rows?: unknown[] } } }>("gene burden", OT_BURDEN_QUERY, { genes, trait });
  const page = result.data?.disease?.evidences;
  if (!Array.isArray(page?.rows)) warnings.push("Open Targets gene-burden response has no usable evidence page; no negative association is inferred.");
  else if (page.cursor || page.count !== page.rows.length) warnings.push("Open Targets gene-burden evidence is a partial page (maximum 300 rows); retain the native cursor/count for further review.");
  return observations.evidence;
}
