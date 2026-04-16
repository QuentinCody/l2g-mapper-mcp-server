/**
 * GTEx Portal v2 REST adapter.
 * Base: https://gtexportal.org/api/v2
 * Used endpoints: /association/singleTissueEqtl, /variant/variantById
 */
export interface GtexFetchOpts {
    fetchImpl?: typeof fetch;
    timeoutMs?: number;
    userAgent?: string;
}
export declare function gtexGet<T = unknown>(path: string, params?: Record<string, unknown>, opts?: GtexFetchOpts): Promise<T>;
/**
 * Fetch single-tissue eQTLs for a batch of variant IDs.
 * `variantIds` may be comma-separated or passed as an array — GTEx accepts multiple values.
 */
export declare function gtexEqtlsByVariants(variantIds: string[], opts?: GtexFetchOpts & {
    tissueSiteDetailId?: string | string[];
    itemsPerPage?: number;
}): Promise<unknown>;
//# sourceMappingURL=gtex.d.ts.map