/**
 * Ensembl REST API adapter.
 * Covers variation, overlap, VEP, and region endpoints commonly used across
 * bio-mcp servers. Promoted from per-server `lib/api-adapter.ts` per the
 * life-science expansion plan §4.3 (Phase 3C).
 */
export type EnsemblBuild = "GRCh37" | "GRCh38";
export interface EnsemblFetchOpts {
    fetchImpl?: typeof fetch;
    timeoutMs?: number;
    build?: EnsemblBuild;
    userAgent?: string;
}
export declare function ensemblGet<T = unknown>(path: string, opts?: EnsemblFetchOpts): Promise<T>;
export declare function ensemblPost<T = unknown>(path: string, body: unknown, opts?: EnsemblFetchOpts): Promise<T>;
//# sourceMappingURL=ensembl.d.ts.map