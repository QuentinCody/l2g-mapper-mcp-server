import { expect, it } from "vitest";
import { hashCanonicalJson } from "@bio-mcp/shared";
import { createCodeModeError, createCodeModeResponse } from "@bio-mcp/shared/codemode/response";
import { McpServer } from "@bio-mcp/shared/mcp";
import { l2gResponse } from "./tool-result";
import { registerQueryData } from "../tools/query-data";
import { registerGetSchema } from "../tools/get-schema";

const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;
function storage(stored: unknown[], fail = false, cell = "fixture") {
  return { idFromName: (name: string) => name, get: () => ({ fetch: async (request: Request) => {
    const path = new URL(request.url).pathname;
    if (path === "/process") {
      stored.push((await request.json() as { data: unknown }).data);
      return Response.json({ success: !fail, tables_created: ["result"], total_rows: 1 }, { status: fail ? 500 : 200 });
    }
    if (path === "/query") return Response.json({ success: true, results: [{ cell }], row_count: 1 });
    return Response.json({ success: true, schema: { tables: { result: { description: cell } } } });
  } }) } as unknown as DurableObjectNamespace;
}

it("adds a verifiable citation to inline Code Mode output without staging", async () => {
  const stored: unknown[] = []; const args = { data_access_id: "evidence" };
  const result = await l2gResponse(async () => createCodeModeResponse({ value: 1 }), { L2G_DATA_DO: storage(stored) }, "mcp_l2g_score", args, {});
  expect(stored).toEqual([]);
  expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, data: { value: 1 }, _meta: { citation: {
    tool: "l2g_score", query_hash: await hashCanonicalJson(args), result_hash: await hashCanonicalJson({ success: true, data: { value: 1 } }), result_scope: "structured_content:root_without_meta",
  } } } });
});

it.each([false, true])("stages full UTF-8 envelopes and preserves failure state (%s)", async failed => {
  const stored: unknown[] = []; const args = { data_access_id: "source-bundle" };
  const data = "α".repeat(16000);
  const result = await l2gResponse(async () => failed ? createCodeModeError("UPSTREAM_ERROR", data) : createCodeModeResponse({ data_access_id: "source-bundle", native: data }), { L2G_DATA_DO: storage(stored) }, "l2g_synthesize", args, {});
  const original = JSON.parse(stored.find((value): value is string => typeof value === "string")!);
  expect(JSON.stringify(original)).toContain(data);
  expect(bytes(original)).toBeGreaterThan(30 * 1024);
  expect(bytes(result)).toBeLessThan(30 * 1024);
  expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: !failed, staged: true, _staging: { payload_size_bytes: bytes(original), payload_hash: `sha256:${await hashCanonicalJson(original)}` }, _meta: { citation: { result_hash: await hashCanonicalJson(original), result_scope: "staged:full_result" } } } });
  expect(result.isError === true).toBe(failed);
  expect(result.structuredContent).not.toHaveProperty("data.native");
  if (!failed) expect(original.structuredContent.data.data_access_id).toBe("source-bundle");
});

it("bounds large content even when the structured data is small", async () => {
  const stored: unknown[] = [];
  const result = await l2gResponse(async () => createCodeModeResponse({ value: 1 }, { textSummary: "x".repeat(120000) }), { L2G_DATA_DO: storage(stored) }, "l2g_score", {}, {});
  expect(result).toMatchObject({ structuredContent: { staged: true } });
  expect(bytes(result)).toBeLessThan(30000);
  expect(JSON.parse(stored.find((value): value is string => typeof value === "string")!).content[0].text).toHaveLength(120000);
});

it.each([false, true])("returns bounded both-fields errors when storage is unavailable or fails (%s)", async present => {
  const result = await l2gResponse(async () => createCodeModeResponse({ cell: "x".repeat(120000) }), present ? { L2G_DATA_DO: storage([], true) } : {}, "l2g_score", {}, {});
  expect(result).toMatchObject({ isError: true, content: expect.any(Array), structuredContent: { success: false, error: { code: "RESPONSE_ERROR" } } });
  expect(bytes(result)).toBeLessThan(1000);
});

it("normalizes a structured failure's MCP flag and bounds serialization failures", async () => {
  const result = await l2gResponse(async () => ({ content: [], structuredContent: { success: false, error: "failed" } }), {}, "l2g_score", {}, {});
  expect(result).toMatchObject({ isError: true, structuredContent: { success: false } });
  const cyclic: Record<string, unknown> = {}; cyclic.self = cyclic;
  expect(await l2gResponse(async () => createCodeModeResponse(cyclic, { textSummary: "fixture" }), {}, "l2g_score", {}, {})).toMatchObject({ isError: true, structuredContent: { success: false } });
});

it.each(["query", "schema"])("bounds both registered %s aliases using real shared handlers", async kind => {
  const stored: unknown[] = []; const server = new McpServer({ name: "fixture", version: "1" });
  const env = { L2G_DATA_DO: storage(stored, false, "α".repeat(60000)) };
  if (kind === "query") registerQueryData(server, env); else registerGetSchema(server, env);
  const tools = (server as unknown as { _registeredTools: Record<string, { handler: (args: unknown, extra: unknown) => Promise<unknown> }> })._registeredTools;
  expect(Object.keys(tools)).toHaveLength(2);
  for (const tool of Object.values(tools)) {
    const result = await tool.handler({ data_access_id: "evidence", sql: "SELECT * FROM result" }, { sessionId: "fixture" });
    expect(result).toMatchObject({ content: expect.any(Array), structuredContent: { success: true, staged: true, _staging: expect.any(Object) } });
    expect(bytes(result)).toBeLessThan(30000);
  }
});
