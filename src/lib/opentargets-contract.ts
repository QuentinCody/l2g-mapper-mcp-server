import { OT_META, type OtMeta } from "./opentargets-observations";

/** Field contracts checked against the provider SDL, September 30, 2026.
 * https://api.platform.opentargets.org/api/v4/graphql/schema */
export const OT_SEARCH_STUDY_QUERY = `query L2GSearch($q: String!) {
  ${OT_META}
  search(queryString: $q, entityNames: ["study"], page: {index: 0, size: 20}) {
    total hits { score object { ... on Study { id projectId hasSumstats studyType traitFromSource traitFromSourceMappedIds } } }
  }
}`;
export const OT_STUDY_CS_QUERY = `query L2GStudy($id: String!) {
  ${OT_META}
  study(studyId: $id) { credibleSets(page: {index: 0, size: 400}) { count rows { studyLocusId variant { id rsIds } } } }
}`;
export const OT_BATCH_DETAIL_QUERY = `query L2GBatch($ids: [String!]!, $size: Int!, $colocSize: Int!) {
  ${OT_META}
  credibleSets(studyLocusIds: $ids, page: {index: 0, size: $size}) {
    count rows {
      studyLocusId studyId studyType finemappingMethod confidence qualityControls variant { id rsIds }
      study { id projectId studyType traitFromSource traitFromSourceMappedIds nSamples nCases nControls
        cohorts discoverySamples { ancestry sampleSize } replicationSamples { ancestry sampleSize }
        qualityControls analysisFlags target { id approvedSymbol } biosample { biosampleId biosampleName } }
      l2GPredictions(page: {index: 0, size: 100}) { count rows { score shapBaseValue features { name value shapValue } target { id approvedSymbol } } }
      colocalisation(page: {index: 0, size: $colocSize}) {
        count rows { colocalisationMethod h3 h4 clpp rightStudyType otherStudyLocus { studyId studyLocusId studyType qtlGeneId isTransQtl qualityControls study { id studyType target { id approvedSymbol } biosample { biosampleId biosampleName } } } }
      }
    }
  }
}`;

export interface L2gPredictionRow {
  locus_id: string; lead_rsid: string; study_locus_id: string | null;
  symbol: string; ensembl_id: string | null; score: number;
  data_version_json: string; model_version: null; variant_id: string | null;
  features_json: string; shap_base_value: number | null; study_json: string; credible_set_json: string;
  genome_assembly: "GRCh38"; anchor_match: "exact_resolved_variant_id";
  requested_trait_id: string | null; trait_match: "exact_mapped_trait_id" | "unmatched" | "unresolved";
}
export interface ColocRow {
  locus_id: string; lead_rsid: string; study_locus_id: string | null;
  method: string | null; h4: number | null; clpp: number | null; other_study_id: string | null;
  lead_variant_id: string | null; genome_assembly: "GRCh38"; anchor_match: "exact_resolved_variant_id";
  data_version_json: string; study_json: string; credible_set_json: string; native_coloc_json: string;
  ensembl_id: string | null; requested_trait_id: string | null;
  trait_match: "exact_mapped_trait_id" | "unmatched" | "unresolved";
}
export interface OtStudy { id: string; projectId?: string; hasSumstats?: boolean; studyType?: string; traitFromSourceMappedIds?: string[] }
export interface OtSearchResp { search?: { total?: number; hits?: Array<{ object?: OtStudy }> } }
export interface OtCsRow { studyLocusId?: string; variant?: { id?: string; rsIds?: string[] } }
export interface OtStudyResp { study?: { credibleSets?: { count?: number; rows?: OtCsRow[] } } }
export interface OtPrediction { score?: number; shapBaseValue?: number; features?: unknown; target?: { id?: string; approvedSymbol?: string } }
export interface OtColoc {
  colocalisationMethod?: string; h4?: number; clpp?: number; rightStudyType?: string;
  otherStudyLocus?: { studyId?: string; studyLocusId?: string; studyType?: string; qtlGeneId?: string; isTransQtl?: boolean;
    study?: { id?: string; studyType?: string; target?: { id?: string }; biosample?: { biosampleId?: string } } };
}
export interface OtDetail extends OtCsRow {
  study?: unknown;
  l2GPredictions?: { count?: number; rows?: OtPrediction[] };
  colocalisation?: { count?: number; rows?: OtColoc[] };
}
export interface OtBatchResp { meta?: OtMeta; credibleSets?: { count?: number; rows?: OtDetail[] } }
