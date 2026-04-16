/**
 * Hidden __sparql_proxy tool — routes V8 isolate sparql.query() calls
 * through the server's SPARQL fetch function.
 *
 * Only callable from V8 isolates (hidden=true). Auto-stages large responses.
 */
import { z } from "zod";
import { shouldStage, stageToDoAndRespond } from "../staging/utils";
const ENVELOPE_SCALAR_LIMIT = 1024;
function preserveEnvelopeScalars(original, staging) {
    if (!original || typeof original !== "object" || Array.isArray(original))
        return;
    for (const [key, value] of Object.entries(original)) {
        if (key in staging)
            continue;
        try {
            const serialized = JSON.stringify(value);
            if (serialized !== undefined && serialized.length <= ENVELOPE_SCALAR_LIMIT) {
                staging[key] = value;
            }
        }
        catch {
            // non-serializable
        }
    }
}
function buildStagedTableSummary(staged) {
    const tables = staged.tablesCreated;
    const rowCounts = staged._staging?.table_row_counts;
    if (!tables || tables.length === 0) {
        return `${staged.totalRows ?? 0} rows`;
    }
    if (tables.length === 1) {
        const rows = rowCounts?.[tables[0]] ?? staged.totalRows ?? 0;
        return `table "${tables[0]}" [${rows} rows]`;
    }
    const details = tables
        .map((t) => {
        const rows = rowCounts?.[t];
        return rows !== undefined ? `${t} [${rows}]` : t;
    })
        .join(", ");
    return `${tables.length} tables: ${details}`;
}
function shapeForStaging(parsed) {
    // For SELECT results: return the bindings array directly so the staging
    // engine can detect tabular shape and create one row per binding.
    if (Array.isArray(parsed?.bindings)) {
        return parsed.bindings.map((b) => {
            const row = {};
            for (const [k, v] of Object.entries(b)) {
                row[k] = v?.value ?? null;
            }
            return row;
        });
    }
    return parsed;
}
async function tryAutoStage(envelope, config) {
    const stageable = shapeForStaging(envelope);
    const responseBytes = JSON.stringify(stageable).length;
    if (!config.doNamespace || !shouldStage(responseBytes, config.threshold)) {
        return undefined;
    }
    const staged = await stageToDoAndRespond(stageable, config.doNamespace, config.prefix, undefined, undefined, config.prefix);
    const tableDetail = buildStagedTableSummary(staged);
    const wrapper = {
        __staged: true,
        data_access_id: staged.dataAccessId,
        schema: staged.schema,
        tables_created: staged.tablesCreated,
        total_rows: staged.totalRows,
        _staging: staged._staging,
        message: `SPARQL response auto-staged (${(responseBytes / 1024).toFixed(1)}KB → ${tableDetail}). Use api.query("${staged.dataAccessId}", sql) in-band, or return this object for the caller to use the query_data tool.`,
    };
    preserveEnvelopeScalars(envelope, wrapper);
    return wrapper;
}
async function executeAndMaybeStage(sparqlFetch, query, method, format, timeoutMs, staging) {
    const raw = (await sparqlFetch(query, { method, format, timeoutMs }));
    const wrapper = await tryAutoStage(raw, staging);
    return wrapper ?? raw;
}
export function createSparqlProxyTool(options) {
    const staging = {
        doNamespace: options.doNamespace,
        prefix: options.stagingPrefix,
        threshold: options.stagingThreshold,
    };
    return {
        name: "__sparql_proxy",
        description: "Route SPARQL queries from V8 isolate through server fetch layer. Internal only.",
        hidden: true,
        schema: {
            query: z.string(),
            method: z.enum(["GET", "POST"]).optional(),
            format: z.string().optional(),
            timeoutMs: z.number().optional(),
        },
        handler: async (input) => {
            const query = String(input.query || "");
            if (!query) {
                return { __sparql_error: true, code: "invalid_input", message: "query is required" };
            }
            const method = input.method ?? "POST";
            const format = input.format ?? "json";
            const timeoutMs = typeof input.timeoutMs === "number" ? input.timeoutMs : 60_000;
            try {
                return await executeAndMaybeStage(options.sparqlFetch, query, method, format, timeoutMs, staging);
            }
            catch (err) {
                const message = err instanceof Error ? err.message : String(err);
                return { __sparql_error: true, code: "execution_error", message };
            }
        },
    };
}
//# sourceMappingURL=sparql-proxy.js.map