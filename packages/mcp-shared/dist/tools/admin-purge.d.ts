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
export interface AdminPurgeResult {
    purged: boolean;
    sqliteSizeBeforeBytes: number | null;
    sqliteSizeAfterBytes: number | null;
}
export interface AdminStatsResult {
    sqliteSizeBytes: number | null;
    tables: string[];
}
export interface AdminCapableStub extends DurableObjectStub {
    __admin_purge__(): Promise<AdminPurgeResult>;
    __admin_stats__(): Promise<AdminStatsResult>;
}
interface DoStorageLike {
    deleteAll(): Promise<void>;
    sql: {
        exec(query: string): {
            toArray(): Array<Record<string, unknown>>;
        };
        databaseSize?: number;
    };
}
/**
 * Worker-side router. Returns a Response if the path is /__admin__/purge/:oid
 * or /__admin__/stats/:oid, null otherwise.
 */
export declare function handleAdminPurge(request: Request, env: Record<string, unknown>, bindingName: string): Promise<Response | null>;
/**
 * DO-side helper. Use inside RPC methods:
 *
 *     async __admin_purge__() { return adminPurgeMixin.purge(this.ctx.storage); }
 *     async __admin_stats__() { return adminPurgeMixin.stats(this.ctx.storage); }
 */
export declare const adminPurgeMixin: {
    purge(storage: DoStorageLike): Promise<AdminPurgeResult>;
    stats(storage: DoStorageLike): AdminStatsResult;
};
export {};
//# sourceMappingURL=admin-purge.d.ts.map