import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { createQueryDataHandler } from "@bio-mcp/shared/staging/utils";

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
					"loci, candidate_genes, l2g_predictions, coloc, eqtl, burden, clinvar, gnomad, " +
					"hpa, scored_candidate_genes, cross_locus_ranked_genes, l2g_meta.",
				inputSchema: {
					data_access_id: z.string().min(1).describe("Data access ID for the staged dataset"),
					sql: z.string().min(1).describe("SELECT/WITH statement to execute"),
					limit: z.number().int().positive().max(10000).default(100).optional(),
				},
			},
			async (args, extra) => {
				const runtimeEnv = env || (extra as { env?: QueryEnv })?.env || {};
				return handler(args as Record<string, unknown>, runtimeEnv as Record<string, unknown>);
			},
		);

	reg("mcp_l2g_query_data");
	reg("l2g_query_data");
}
