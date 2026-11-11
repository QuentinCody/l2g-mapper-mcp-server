import { RestStagingDO } from "@bio-mcp/shared/staging/rest-staging-do";
import type { SchemaHints } from "@bio-mcp/shared/staging/schema-inference";

/**
 * L2G staging DO. Holds tables populated by `l2g_gather` and the derived
 * scoring tables written by `l2g_score`.
 *
 * Expected tables (all share a single data_access_id):
 *   - anchors, loci, candidate_genes
 *   - l2g_predictions, coloc, eqtl, burden, genebass_evidence
 *   - clinvar, clinvar_evidence, gnomad, hpa (optional opt-in evidence)
 *   - scored_candidate_genes, cross_locus_ranked_genes  (written by l2g_score)
 */
export class L2gDataDO extends RestStagingDO {
	protected replaceMaterializedTables(data: unknown): boolean {
		if (!data || typeof data !== "object" || Array.isArray(data)) return false;
		const tables = data as Record<string, unknown>;
		if (Object.keys(tables).sort().join(",") !== "cross_locus_ranked_genes,scored_candidate_genes") return false;
		for (const rows of Object.values(tables)) {
			if (!Array.isArray(rows) || !rows.length || rows.some(row => !row || typeof row !== "object" || Array.isArray(row) ||
				Object.values(row).some(value => value !== null && typeof value === "object"))) throw new Error("Derived score tables require nonempty, flat rows");
		}
		return true;
	}

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
