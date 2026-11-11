import { buildCitation } from "@bio-mcp/shared";
import { stageToDoAndRespond } from "@bio-mcp/shared/staging/utils";
import { createCodeModeResponse } from "@bio-mcp/shared/codemode/response";
import { GATHER_POLICY_VERSION } from "./evidence-policy";

interface GatherResultInput {
  tables: Record<string, unknown[]>;
  meta: { trait_query: string | null; efo_id: string | null; efo_label: string | null; generated_at: string; sources_queried: string[]; caps: Record<string, unknown>; target_gene: string | null };
  warnings: string[]; args: unknown; elapsedMs: number; namespace: DurableObjectNamespace; extra: unknown;
}

function traitSummary(meta: GatherResultInput["meta"]) {
  const preview = (value: string | null) => value && value.length > 500 ? value.slice(0, 500) + "…" : value;
  return { query: preview(meta.trait_query), efo_id: preview(meta.efo_id), label: preview(meta.efo_label),
    display_truncated: [meta.trait_query, meta.efo_id, meta.efo_label].some(value => value && value.length > 500) };
}

/** Stage metadata and native observations in the same write as the ranked inputs. */
export async function stageGatherResult(input: GatherResultInput) {
  const { tables, meta, warnings } = input;
  const payload = { ...tables, l2g_meta: [{
    gather_policy_version: GATHER_POLICY_VERSION,
    trait_query: meta.trait_query, efo_id: meta.efo_id, efo_label: meta.efo_label,
    generated_at: meta.generated_at, sources_queried: meta.sources_queried.join(" | "),
    target_gene: meta.target_gene, caps_json: JSON.stringify(meta.caps), warnings_json: JSON.stringify(warnings),
  }] };
  const staged = await stageToDoAndRespond(payload, input.namespace, "l2g", undefined,
    { toolName: "l2g_gather", serverName: "l2g-mapper-mcp-server" }, "l2g", input.extra as Parameters<typeof stageToDoAndRespond>[6]);
  const citation = await buildCitation({
    source: { id: "l2g", name: "L2G gathered evidence; consult per-source observations" },
    server: "l2g", tool: "l2g_gather", query: input.args, queryScope: "tool_arguments",
    result: payload, resultScope: "staged:full_result", dataAccessId: staged.dataAccessId, retrievedAt: meta.generated_at,
  });
  const largeWarnings = new TextEncoder().encode(JSON.stringify(warnings)).byteLength > 15000;
  return createCodeModeResponse({
    data_access_id: staged.dataAccessId, trait: { ...traitSummary(meta), resolution_status: (tables.trait_resolution?.[0] as { status?: string } | undefined)?.status ?? "unknown" },
    anchors_count: tables.anchors.length, loci_count: tables.loci.length, candidate_genes_count: tables.candidate_genes.length,
    elapsed_ms: input.elapsedMs, warnings: largeWarnings ? undefined : warnings,
    warnings_staged: largeWarnings, warning_count: warnings.length,
    _staging: staged._staging, next_step: `Call l2g_score with data_access_id='${staged.dataAccessId}'.`,
  }, {
    textSummary: `l2g_gather staged ${tables.anchors.length} anchors / ${tables.loci.length} loci. Inspect opentargets_evidence and l2g_meta for native release evidence and limitations.`,
    meta: { citation, staged: true, data_access_id: staged.dataAccessId, tables_created: staged.tablesCreated, total_rows: staged.totalRows, payload_hash: staged._staging.payload_hash },
  });
}
