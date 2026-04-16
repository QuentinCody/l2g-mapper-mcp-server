/**
 * Administrative purge handlers for Durable Objects.
 *
 * Design: Worker-side helper calls RPC methods on the DO stub rather than
 * proxying HTTP into the DO. This avoids any risk of interfering with the
 * McpAgent streaming-HTTP transport, which owns the DO's `fetch()`.
 *
 * Usage from a Worker's default `fetch()`:
 *
 *     import { handleAdminPurge } from "@bio-mcp/shared/tools/admin-purge";
 *     export default {
 *         async fetch(request, env, ctx) {
 *             const adminResp = await handleAdminPurge(request, env, "MCP_OBJECT");
 *             if (adminResp) return adminResp;
 *             // ...existing routing...
 *         }
 *     };
 *
 * The target DO class must implement these async RPC methods:
 *
 *     async __admin_purge__(): Promise<AdminPurgeResult>
 *     async __admin_stats__(): Promise<AdminStatsResult>
 *
 * Use `adminPurgeMixin.purge(this.ctx.storage)` etc. for a one-line impl.
 *
 * Security: requires the ADMIN_TOKEN secret on the Worker. Missing → 503.
 * Mismatch → 403. Set via `wrangler secret put ADMIN_TOKEN`.
 */
const OID_PATTERN = /^[a-f0-9]{64}$/;
/**
 * Worker-side router. Returns a Response if the path is /__admin__/purge/:oid
 * or /__admin__/stats/:oid, null otherwise.
 */
export async function handleAdminPurge(request, env, bindingName) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/__admin__\/(purge|stats)\/([a-f0-9]+)$/);
    if (!match)
        return null;
    const [, op, oid] = match;
    if (!OID_PATTERN.test(oid)) {
        return jsonResponse({ error: "invalid object id", expected: "64 lowercase hex chars" }, 400);
    }
    const adminToken = env.ADMIN_TOKEN;
    if (!adminToken) {
        return jsonResponse({ error: "admin disabled", reason: "ADMIN_TOKEN secret not set" }, 503);
    }
    if (request.headers.get("x-admin-token") !== adminToken) {
        return jsonResponse({ error: "forbidden" }, 403);
    }
    const namespace = env[bindingName];
    if (!namespace || typeof namespace.idFromString !== "function") {
        return jsonResponse({ error: "binding not found", bindingName }, 500);
    }
    let id;
    try {
        id = namespace.idFromString(oid);
    }
    catch (err) {
        return jsonResponse({ error: "could not resolve id", message: err.message }, 400);
    }
    const stub = namespace.get(id);
    if (op === "purge" && request.method !== "DELETE") {
        return jsonResponse({ error: "method not allowed", expected: "DELETE" }, 405);
    }
    if (op === "stats" && request.method !== "GET") {
        return jsonResponse({ error: "method not allowed", expected: "GET" }, 405);
    }
    try {
        const result = op === "purge"
            ? await stub.__admin_purge__()
            : await stub.__admin_stats__();
        return jsonResponse(result);
    }
    catch (err) {
        return jsonResponse({ error: "admin rpc failed", op, message: err.message }, 500);
    }
}
/**
 * DO-side helper. Use inside RPC methods:
 *
 *     async __admin_purge__() { return adminPurgeMixin.purge(this.ctx.storage); }
 *     async __admin_stats__() { return adminPurgeMixin.stats(this.ctx.storage); }
 */
export const adminPurgeMixin = {
    async purge(storage) {
        const before = safeDatabaseSize(storage);
        await storage.deleteAll();
        return {
            purged: true,
            sqliteSizeBeforeBytes: before,
            sqliteSizeAfterBytes: safeDatabaseSize(storage),
        };
    },
    stats(storage) {
        let tables = [];
        try {
            tables = storage.sql
                .exec("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .toArray()
                .map((r) => String(r.name));
        }
        catch {
            // empty or non-SQL storage
        }
        return { sqliteSizeBytes: safeDatabaseSize(storage), tables };
    },
};
function safeDatabaseSize(storage) {
    try {
        return typeof storage.sql.databaseSize === "number"
            ? storage.sql.databaseSize
            : null;
    }
    catch {
        return null;
    }
}
function jsonResponse(body, status = 200) {
    return new Response(JSON.stringify(body), {
        status,
        headers: { "content-type": "application/json" },
    });
}
//# sourceMappingURL=admin-purge.js.map