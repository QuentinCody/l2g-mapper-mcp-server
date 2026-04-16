import { RestStagingDO } from "@bio-mcp/shared/staging/rest-staging-do";
import type { SchemaHints } from "@bio-mcp/shared/staging/schema-inference";

/**
 * L2G staging DO. Holds tables populated by `l2g_gather` and the derived
 * scoring tables written by `l2g_score`.
 *
 * Expected tables (all share a single data_access_id):
 *   - anchors, loci, candidate_genes
 *   - l2g_predictions, coloc, eqtl, burden
 *   - clinvar, gnomad, hpa  (optional — set only when opt-in lanes enabled)
 *   - scored_candidate_genes, cross_locus_ranked_genes  (written by l2g_score)
 */
export class L2gDataDO extends RestStagingDO {
	protected getSchemaHints(data: unknown): SchemaHints | undefined {
		if (!data || typeof data !== "object") return undefined;
		const obj = data as Record<string, unknown>;

		// l2g_gather payload wraps {tables: {anchors: [], loci: [], ...}}
		if ("tables" in obj && obj.tables && typeof obj.tables === "object") {
			// The staging engine will iterate each top-level array; we return hints
			// for the most-common primary table.
			return {
				tableName: "l2g_bundle",
				indexes: ["rsid", "locus_id", "symbol"],
			};
		}

		return undefined;
	}
}
