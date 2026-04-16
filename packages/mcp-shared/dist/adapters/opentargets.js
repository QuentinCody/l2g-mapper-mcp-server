/**
 * Open Targets Platform GraphQL adapter.
 * Endpoint: https://api.platform.opentargets.org/api/v4/graphql
 */
const OT_GRAPHQL = "https://api.platform.opentargets.org/api/v4/graphql";
export async function opentargetsGraphql(query, variables = {}, opts = {}) {
    const { fetchImpl = fetch, timeoutMs = 20_000, userAgent = "bio-mcp-opentargets/1.0", endpoint = OT_GRAPHQL, } = opts;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
        const resp = await fetchImpl(endpoint, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                Accept: "application/json",
                "User-Agent": userAgent,
            },
            body: JSON.stringify({ query, variables }),
            signal: ctrl.signal,
        });
        if (!resp.ok) {
            throw new Error(`OpenTargets HTTP ${resp.status}: ${await resp.text().catch(() => "")}`);
        }
        return (await resp.json());
    }
    finally {
        clearTimeout(timer);
    }
}
//# sourceMappingURL=opentargets.js.map