import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { createGetSchemaHandler } from "@bio-mcp/shared/staging/utils";

interface SchemaEnv {
	L2G_DATA_DO?: unknown;
}

export function registerGetSchema(server: McpServer, env?: SchemaEnv): void {
	const handler = createGetSchemaHandler("L2G_DATA_DO", "l2g");
	const reg = (name: string) =>
		server.registerTool(
			name,
			{
				title: "Get L2G Staged Data Schema",
				description:
					"Inspect the schema for a staged L2G bundle (tables: anchors, loci, " +
					"candidate_genes, l2g_predictions, coloc, eqtl, burden, clinvar, gnomad, hpa, " +
					"plus scored_candidate_genes and cross_locus_ranked_genes after l2g_score runs). " +
					"If called without a data_access_id, lists all staged datasets in this session.",
				inputSchema: {
					data_access_id: z
						.string()
						.min(1)
						.optional()
						.describe("Data access ID for the staged dataset. Omit to list session datasets."),
				},
			},
			async (args, extra) => {
				const runtimeEnv = env || (extra as { env?: SchemaEnv })?.env || {};
				return handler(
					args as Record<string, unknown>,
					runtimeEnv as Record<string, unknown>,
					(extra as { sessionId?: string })?.sessionId,
				);
			},
		);

	reg("mcp_l2g_get_schema");
	reg("l2g_get_schema");
}
