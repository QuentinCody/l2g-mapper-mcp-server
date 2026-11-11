import { buildCitation } from "@bio-mcp/shared";
import { createCodeModeError, type CodeModeResponse } from "@bio-mcp/shared/codemode/response";
import { shouldStage, stageToDoAndRespond } from "@bio-mcp/shared/staging/utils";

type Env = { L2G_DATA_DO?: unknown };
const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;
const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);
const source = { id: "l2g", name: "L2G derived results and staged evidence" };

async function citeInline(result: CodeModeResponse, tool: string, args: unknown) {
  const body = record(result.structuredContent) ? result.structuredContent : { data: result.structuredContent };
  const failed = result.isError === true || body.success === false;
  const structured: Record<string, unknown> = { ...body, success: !failed };
  const meta = record(body._meta) ? body._meta : {};
  const { _meta: _ignored, ...withoutMeta } = structured;
  const citation = meta.citation ?? await buildCitation({
    source, server: "l2g", tool, query: args, queryScope: "tool_arguments",
    result: withoutMeta, resultScope: "structured_content:root_without_meta", retrievedAt: new Date().toISOString(),
  });
  return { ...result, structuredContent: { ...structured, _meta: { ...meta, citation } }, ...(failed ? { isError: true } : {}) };
}

/** Bound the complete tool envelope, including SQL cells, figures, metadata and errors. */
export async function l2gResponse(work: () => Promise<CodeModeResponse>, env: Env, name: string, args: unknown, extra: unknown): Promise<CodeModeResponse> {
  const tool = name.replace(/^mcp_/, "");
  try {
    const original = await citeInline(await work(), tool, args);
    const originalBytes = bytes(original);
    if (!shouldStage(originalBytes)) return original;
    if (!env.L2G_DATA_DO) throw new Error("Missing staging storage");
    const staged = await stageToDoAndRespond(original, env.L2G_DATA_DO as Parameters<typeof stageToDoAndRespond>[1], "l2g", undefined,
      { toolName: tool, serverName: "l2g-mapper-mcp-server" }, "l2g", extra as Parameters<typeof stageToDoAndRespond>[6]);
    const citation = await buildCitation({
      source, server: "l2g", tool, query: args, queryScope: "tool_arguments", result: original,
      resultScope: "staged:full_result", dataAccessId: staged.dataAccessId, retrievedAt: new Date().toISOString(),
    });
    const structuredContent = {
      success: original.isError !== true, staged: true, data: undefined,
      data_access_id: staged.dataAccessId, _staging: { ...staged._staging, payload_size_bytes: originalBytes },
      _meta: { citation, staged: true, data_access_id: staged.dataAccessId },
    };
    if (bytes(structuredContent) > 100000) throw new Error("Staging metadata exceeds transport limit");
    return {
      content: [{ type: "text", text: "L2G response staged. Use l2g_get_schema and l2g_query_data with this response's data_access_id; the payloads table preserves the complete result, including its original evidence handle." }],
      structuredContent, ...(original.isError ? { isError: true } : {}),
    };
  } catch {
    return createCodeModeError("RESPONSE_ERROR", "L2G response could not be returned; storage or serialization failed.");
  }
}
