/**
 * gnomAD GraphQL adapter.
 * Endpoint: https://gnomad.broadinstitute.org/api
 */
const GNOMAD_GRAPHQL = "https://gnomad.broadinstitute.org/api";
export async function gnomadGraphql(query, variables = {}, opts = {}) {
    const { fetchImpl = fetch, timeoutMs = 20_000, userAgent = "bio-mcp-gnomad/1.0" } = opts;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
        const resp = await fetchImpl(GNOMAD_GRAPHQL, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                Accept: "application/json",
                "User-Agent": userAgent,
            },
            body: JSON.stringify({ query, variables }),
            signal: ctrl.signal,
        });
        if (!resp.ok)
            throw new Error(`gnomAD HTTP ${resp.status}`);
        return (await resp.json());
    }
    finally {
        clearTimeout(timer);
    }
}
//# sourceMappingURL=gnomad.js.map