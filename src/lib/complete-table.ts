import { queryDataFromDo } from "@bio-mcp/shared/staging/utils";

/** Read immutable gather/score inputs without dropping oversized cells or rows.
 * SQL and table names come only from the composer's own call sites. */
export async function readCompleteTable<T>(namespace: DurableObjectNamespace, id: string, table: string, sql = `SELECT * FROM ${table}`): Promise<T[]> {
  const allowed = ["opentargets_evidence", "scored_candidate_genes", "hpa", "gnomad", "clinvar_evidence", "eqtl_catalogue_evidence", "eqtl_catalogue_r8_evidence", "genebass_evidence", "finngen_evidence", "finngen_phenotype_evidence"];
  if (!allowed.includes(table)) throw new Error("Unsupported chunked composer table");
  let calls = 0;
  const maxCalls = table === "finngen_evidence" ? 3300 : table === "genebass_evidence" ? 2400 : 300;
  async function query(statement: string, limit = 10000, whole = true) {
    if (++calls > maxCalls) throw new Error(`Complete composer read exceeds ${maxCalls}-query budget; no partial evidence returned`);
    const result = await queryDataFromDo(namespace, id, statement, limit);
    if (whole && (result.truncated || result.count_capped || (result.total_matching != null && result.total_matching > result.rows.length))) throw new Error(`Complete ${table} input required; SQL read was capped`);
    return result.rows as Record<string, unknown>[];
  }
  try { return await query(sql) as T[]; }
  catch (error) {
    if (!(error instanceof Error) || !error.message.includes("LOSSLESS_QUERY_RESULT_TOO_LARGE")) throw error;
  }
  const schema = await query(`PRAGMA table_info('${table}')`);
  if (!schema.length || schema.length > 100 || schema.some(row => typeof row.name !== "string")) throw new Error("Cannot reconstruct composer row without its column schema");
  const columns = schema.map(row => {
    const name = row.name as string;
    return `'${name.replace(/'/g, "''")}', "${name.replace(/"/g, '""')}"`;
  }).join(", ");
  const expression = `json_object(${columns})`;
  return await readCompleteRows<T>(query, sql, expression);
}

async function readCompleteRows<T>(query: (statement: string, limit?: number, whole?: boolean) => Promise<Record<string, unknown>[]>, sql: string, expression: string) {
  const totals = await query(`SELECT COUNT(*) AS total FROM (${sql})`);
  const count = totals[0]?.total;
  if (totals.length !== 1 || !Number.isSafeInteger(count) || Number(count) < 0 || Number(count) > 10000) throw new Error("Invalid complete composer row count");
  const rows: T[] = []; let bytes = 0;
  for (let index = 0; index < Number(count); index++) {
    const source = `FROM (${sql}) LIMIT 1 OFFSET ${index}`;
    const lengths = await query(`SELECT length(${expression}) AS size ${source}`, 1, false);
    const length = lengths[0]?.size;
    if (lengths.length !== 1 || !Number.isSafeInteger(length) || Number(length) < 2 || Number(length) > 24 * 1024 * 1024) throw new Error("Invalid or oversized composer row length");
    const parts: string[] = [];
    for (let start = 1; start <= Number(length); start += 8192) {
      const result = await query(`SELECT substr(${expression}, ${start}, 8192) AS part ${source}`, 1, false);
      if (result.length !== 1 || typeof result[0].part !== "string" || [...result[0].part].length !== Math.min(8192, Number(length) - start + 1)) throw new Error("Incomplete composer row chunk");
      bytes += new TextEncoder().encode(result[0].part).byteLength;
      if (bytes > 24 * 1024 * 1024) throw new Error("Complete composer read exceeds 24 MiB; no partial evidence returned");
      parts.push(result[0].part);
    }
    rows.push(JSON.parse(parts.join("")) as T);
  }
  return rows;
}
