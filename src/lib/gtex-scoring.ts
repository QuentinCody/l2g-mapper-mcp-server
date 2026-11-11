import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";
import type { EqtlRow } from "./gtex-evidence";
import { qualifiedGtex } from "./gtex-qualified";

async function read(namespace: DurableObjectNamespace, id: string, table: string) {
  const result = await queryDataFromDo(namespace, id, `SELECT * FROM ${table}`, 10000);
  if (result.truncated || result.count_capped || (result.total_matching != null && result.total_matching > result.rows.length)) throw new Error(`GTEx qualification requires complete ${table} input`);
  return result.rows as Record<string, unknown>[];
}

export async function loadGtexSupport(namespace: DurableObjectNamespace, id: string, tables: string[] = []) {
  const rows = tables.includes("eqtl") ? await read(namespace, id, "eqtl") : [];
  const observations = tables.includes("gtex_evidence") ? await read(namespace, id, "gtex_evidence") : [];
  const anchors = await read(namespace, id, "anchors");
  const meta = await read(namespace, id, "l2g_meta");
  if (meta.length !== 1) throw new Error("GTEx qualification requires one gather metadata row");
  const caps = JSON.parse(String(meta[0].caps_json ?? "{}"));
  return { ...await qualifiedGtex(rows as unknown as EqtlRow[], observations, anchors, caps?.gtex_dataset_id, id),
    anchors, caps_json: String(meta[0].caps_json ?? "{}") };
}
