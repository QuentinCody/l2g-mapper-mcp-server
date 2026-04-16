import { McpAgent } from "agents/mcp";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { registerL2gGather } from "./tools/l2g-gather";
import { registerL2gScore } from "./tools/l2g-score";
import { registerL2gSynthesize } from "./tools/l2g-synthesize";
import { registerGetSchema } from "./tools/get-schema";
import { registerQueryData } from "./tools/query-data";
import { L2gDataDO } from "./do";

export { L2gDataDO };

interface L2gEnv {
	L2G_DATA_DO: DurableObjectNamespace;
}

export class MyMCP extends McpAgent {
	server = new McpServer({
		name: "l2g-mapper",
		version: "0.1.0",
	});

	async init() {
		const env = this.env as unknown as L2gEnv;
		registerL2gGather(this.server, env);
		registerL2gScore(this.server, env);
		registerL2gSynthesize(this.server, env);
		registerGetSchema(this.server, env);
		registerQueryData(this.server, env);
	}
}

export default {
	fetch(request: Request, env: Env, ctx: ExecutionContext) {
		const url = new URL(request.url);

		if (url.pathname === "/health") {
			return new Response("ok", {
				status: 200,
				headers: { "content-type": "text/plain" },
			});
		}

		if (url.pathname === "/mcp") {
			return MyMCP.serve("/mcp", { binding: "MCP_OBJECT" }).fetch(request, env, ctx);
		}

		return new Response("Not found", { status: 404 });
	},
};
