/**
 * Genebass REST adapter.
 * Base: https://app.genebass.org/api
 * Gene burden PheWAS by Ensembl gene ID.
 */
const GENEBASS_BASE = "https://app.genebass.org/api";
export async function genebassGeneBurden(ensemblGeneId, burdenSet = "pLoF", opts = {}) {
    const { fetchImpl = fetch, timeoutMs = 20_000, userAgent = "bio-mcp-genebass/1.0", params = {} } = opts;
    const url = new URL(`${GENEBASS_BASE}/gene_burden_phewas`);
    url.searchParams.set("gene_id", ensemblGeneId);
    url.searchParams.set("burden_set", burdenSet);
    for (const [k, v] of Object.entries(params)) {
        if (v == null)
            continue;
        url.searchParams.set(k, String(v));
    }
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
        const resp = await fetchImpl(url.toString(), {
            headers: { Accept: "application/json", "User-Agent": userAgent },
            signal: ctrl.signal,
        });
        if (!resp.ok) {
            throw new Error(`Genebass HTTP ${resp.status}: ${await resp.text().catch(() => "")}`);
        }
        return await resp.json();
    }
    finally {
        clearTimeout(timer);
    }
}
//# sourceMappingURL=genebass.js.map