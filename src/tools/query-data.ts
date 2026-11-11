import type { McpServer } from "@bio-mcp/shared/mcp";
import { z } from "zod";
import { createQueryDataHandler } from "@bio-mcp/shared/staging/utils";
import { l2gResponse } from "../lib/tool-result";

interface QueryEnv {
	L2G_DATA_DO?: unknown;
}

export function registerQueryData(server: McpServer, env?: QueryEnv): void {
	const handler = createQueryDataHandler("L2G_DATA_DO", "l2g");
	const reg = (name: string) =>
		server.registerTool(
			name,
			{
				title: "Query Staged L2G Data",
				description:
					"Run read-only SQL against a staged L2G bundle. Useful tables: anchors, " +
					"loci, candidate_genes, candidate_selection, l2g_predictions, coloc, eqtl, burden, clinvar, clinvar_evidence, gnomad, " +
					"hpa, finngen_evidence, finngen_phenotype_evidence, trait_resolution, coordinate_resolution, opentargets_evidence, scored_candidate_genes, cross_locus_ranked_genes, l2g_meta.",
				inputSchema: {
					data_access_id: z.string().min(1).describe("Data access ID for the staged dataset"),
					sql: z.string().min(1).describe("SELECT/WITH statement to execute"),
					limit: z.number().int().positive().max(10000).default(100).optional(),
				},
			},
			async (args, extra) => {
				const runtimeEnv = env || (extra as { env?: QueryEnv })?.env || {};
				return l2gResponse(() => handler(args as Record<string, unknown>, runtimeEnv as Record<string, unknown>), runtimeEnv, name, args, extra);
			},
		);

	reg("mcp_l2g_query_data");
	reg("l2g_query_data");
}
