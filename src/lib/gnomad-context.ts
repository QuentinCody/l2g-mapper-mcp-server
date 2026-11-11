import { gnomadGraphql } from "@bio-mcp/shared/adapters/gnomad";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { boundedJsonFetch } from "./bounded-json-fetch";
import { gnomadProjection } from "./gnomad-projection";

export const GNOMAD_CONTEXT_VERSION = "2026-10-06-native-gene";

export const GNOMAD_GENE_QUERY = `query L2GGene($id: String!, $ref: ReferenceGenomeId!) {
  gene(gene_id: $id, reference_genome: $ref) {
    gene_id symbol reference_genome flags canonical_transcript_id
    mane_select_transcript { ensembl_id ensembl_version refseq_id refseq_version }
    gnomad_constraint { pli oe_lof oe_lof_lower oe_lof_upper oe_lof_percentile lof_z mis_z flags }
  }
}`;
export const GNOMAD_CONSTRAINT_PROVENANCE = {
  reference_genome: "GRCh38", graphql_field: "Gene.gnomad_constraint",
  documented_metric_release: "4.1.1", release_basis: "provider_pipeline",
  pipeline_source: "https://github.com/broadinstitute/gnomad-browser/blob/5e1ff276646072ff8cfa96426bbf8f5237e946c1/data-pipeline/src/data_pipeline/pipelines/genes.py",
  mapping_verified_on: "2026-09-26", runtime_metric_release: null, release_pinned: false,
  published_release_notice: {
    release: "4.1.2", published_on: "2026-09-28",
    source_url: "https://gnomad.broadinstitute.org/news/2026-09-gnomad-v4-1-2/",
    summary: "Constraint-file ranks and bins corrected from 4.1.1; LOEUF values, variant frequencies and 4.1.1 LOEUF percentile thresholds unchanged. Use 4.1.2 constraint files for corrected ranks. Returned GraphQL metrics are preserved without recalculation.",
    runtime_application_verified: false,
  },
  limitation: "Documented pipeline mapping, not a runtime metric-release attestation. The API selects by assembly; no immutable minor-release selector is exposed.",
};
const readGnomadResponse = boundedJsonFetch("gnomAD", 256 * 1024, 4000);
export const boundedGnomadFetch: typeof fetch = async (input, init) => {
  const response = await readGnomadResponse(input, init);
  if ((response.ok && response.status !== 200) || response.headers.has("content-range")) {
    await response.body?.cancel();
    throw new Error("gnomAD requires a complete HTTP 200 response");
  }
  return response;
};
export interface GnomadRow {
  requested_ensembl_id: string; ensembl_id: string | null; symbol: string | null;
  reference_genome: string | null; reference_genome_requested: "GRCh38";
  oe_lof: number | null; oe_lof_lower: number | null; oe_lof_upper: number | null;
  oe_lof_percentile: number | null; lof_z: number | null; mis_z: number | null; pli: number | null;
  gene_flags_json: string; constraint_flags_json: string;
  canonical_transcript_id: string | null; mane_select_transcript_json: string;
  constraint_provenance_json: string; constraint_available: boolean | null;
  source_url: string; query: string; variables_json: string; retrieved_at: string;
  native_response_json: string | null; response_sha256: string | null; usable: boolean; error: string | null;
}
async function fetchGene(id: string, warnings: string[]): Promise<GnomadRow> {
  const variables = { id, ref: "GRCh38" };
  const row: GnomadRow = {
    requested_ensembl_id: id, ensembl_id: null, symbol: null, reference_genome: null, reference_genome_requested: "GRCh38",
    oe_lof: null, oe_lof_lower: null, oe_lof_upper: null, oe_lof_percentile: null, lof_z: null, mis_z: null, pli: null,
    gene_flags_json: "null", constraint_flags_json: "null", canonical_transcript_id: null, mane_select_transcript_json: "null",
    constraint_provenance_json: JSON.stringify(GNOMAD_CONSTRAINT_PROVENANCE), constraint_available: null,
    source_url: "https://gnomad.broadinstitute.org/api", query: GNOMAD_GENE_QUERY, variables_json: JSON.stringify(variables),
    retrieved_at: new Date().toISOString(), native_response_json: null, response_sha256: null, usable: false, error: null,
  };
  try {
    const response = await gnomadGraphql(GNOMAD_GENE_QUERY, variables, { fetchImpl: boundedGnomadFetch, timeoutMs: 4000 });
    row.native_response_json = JSON.stringify(response); row.response_sha256 = await hashCanonicalJson(response);
    row.retrieved_at = new Date().toISOString();
    Object.assign(row, gnomadProjection(response, id), { usable: true });
  } catch (error) {
    row.error = error instanceof Error ? error.message.slice(0, 500) : "gnomAD request failed";
    warnings.push(`gnomAD ${id}: ${row.error}`);
  }
  return row;
}

/** Optional annotation only; never adds a constraint-based scoring contribution. */
export async function fetchGnomadLane(genes: Array<{ ensembl_id: string | null }>, warnings: string[]) {
  const ids = [...new Set(genes.map(g => g.ensembl_id).filter((id): id is string => Boolean(id)))];
  if (genes.some(g => !g.ensembl_id)) warnings.push("gnomAD: candidates without resolved gene IDs skipped; no symbol fallback");
  if (ids.length > 40) warnings.push("gnomAD context capped at 40 distinct candidate genes; coverage is incomplete");
  return Promise.all(ids.slice(0, 40).map(id => fetchGene(id, warnings)));
}
