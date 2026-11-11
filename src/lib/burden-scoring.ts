import { qualifiedBurden } from "./burden-qualified";
import { readCompleteTable } from "./complete-table";
import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";

export async function loadBurdenSupport(namespace: DurableObjectNamespace, id: string, tables: string[] = [], trait: string | null = null) {
  if (!tables.includes("opentargets_evidence")) return qualifiedBurden([], trait, id);
  const probe = await queryDataFromDo(namespace, id, "SELECT phase FROM opentargets_evidence WHERE phase = 'gene burden' LIMIT 1", 1);
  if (!probe.rows.length) return qualifiedBurden([], trait, id);
  const rows = await readCompleteTable<Record<string, unknown>>(namespace, id, "opentargets_evidence");
  return qualifiedBurden(rows, trait, id);
}
