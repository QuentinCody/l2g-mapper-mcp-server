/**
 * Genebass REST adapter.
 * Base: https://app.genebass.org/api
 * Gene burden PheWAS by Ensembl gene ID.
 */
export interface GenebassFetchOpts {
    fetchImpl?: typeof fetch;
    timeoutMs?: number;
    userAgent?: string;
}
export declare function genebassGeneBurden(ensemblGeneId: string, burdenSet?: string, opts?: GenebassFetchOpts & {
    params?: Record<string, unknown>;
}): Promise<unknown>;
//# sourceMappingURL=genebass.d.ts.map