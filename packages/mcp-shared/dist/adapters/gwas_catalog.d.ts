/**
 * GWAS Catalog REST adapter.
 * Base: https://www.ebi.ac.uk/gwas/rest/api
 * Covers trait/association/study endpoints used to anchor L2G runs.
 */
export interface GwasFetchOpts {
    fetchImpl?: typeof fetch;
    timeoutMs?: number;
    userAgent?: string;
}
export declare function gwasGet<T = unknown>(path: string, params?: Record<string, unknown>, opts?: GwasFetchOpts): Promise<T>;
/** Fetch v2 associations filtered by EFO trait or free-text term. */
export declare function gwasAssociations(params: {
    efoTrait?: string;
    traitName?: string;
    size?: number;
    page?: number;
}, opts?: GwasFetchOpts): Promise<unknown>;
//# sourceMappingURL=gwas_catalog.d.ts.map