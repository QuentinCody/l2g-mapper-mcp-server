/**
 * REST HTTP fetch utility with retry, timeout, query string construction,
 * per-source rate limiting, and optional Worker-side caching.
 */
export interface RestFetchOptions {
    method?: string;
    headers?: Record<string, string>;
    body?: string | object;
    timeout?: number;
    retries?: number;
    retryOn?: number[];
    userAgent?: string;
    /** Rate-limit policy key — requests sharing the same key are serialized */
    rateLimitKey?: string;
    /** Cache options for Worker-side caching via Cloudflare Cache API */
    cache?: CacheOptions;
}
export interface CacheOptions {
    /** Time-to-live in seconds (default: 300 = 5 minutes) */
    ttl?: number;
    /** If true, bypass cache and fetch fresh (but still store result) */
    bypass?: boolean;
}
/** Per-source rate-limit policy */
export interface RateLimitPolicy {
    /** Unique key for this source (e.g. "opentargets", "pubmed") */
    key: string;
    /** Minimum interval between requests in milliseconds */
    minIntervalMs: number;
}
/**
 * Register a rate-limit policy for a source key.
 * Call at server startup for each upstream API that needs throttling.
 */
export declare function registerRateLimitPolicy(policy: RateLimitPolicy): void;
/**
 * Clear all registered rate-limit policies and timestamps. For testing only.
 */
export declare function resetRateLimitState(): void;
/**
 * Build a query string from a params object. Handles arrays, undefined values.
 */
export declare function buildQueryString(params: Record<string, unknown>): string;
/**
 * Fetch a REST API with retries, timeout, query string construction,
 * optional per-source rate limiting, and Worker-side caching.
 */
export declare function restFetch(baseUrl: string, path: string, params?: Record<string, unknown>, opts?: RestFetchOptions): Promise<Response>;
//# sourceMappingURL=rest-fetch.d.ts.map