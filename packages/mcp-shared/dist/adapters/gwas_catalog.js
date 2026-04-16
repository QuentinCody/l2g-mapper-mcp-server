/**
 * GWAS Catalog REST adapter.
 * Base: https://www.ebi.ac.uk/gwas/rest/api
 * Covers trait/association/study endpoints used to anchor L2G runs.
 */
const GWAS_BASE = "https://www.ebi.ac.uk/gwas/rest/api";
export async function gwasGet(path, params = {}, opts = {}) {
    const { fetchImpl = fetch, timeoutMs = 30_000, userAgent = "bio-mcp-gwas/1.0" } = opts;
    const url = new URL(path.startsWith("http") ? path : `${GWAS_BASE}${path}`);
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
        if (!resp.ok)
            throw new Error(`GWAS Catalog HTTP ${resp.status}`);
        return (await resp.json());
    }
    finally {
        clearTimeout(timer);
    }
}
/** Fetch v2 associations filtered by EFO trait or free-text term. */
export async function gwasAssociations(params, opts = {}) {
    const qp = { size: params.size ?? 200, page: params.page ?? 0 };
    if (params.efoTrait)
        qp.efoTrait = params.efoTrait;
    if (params.traitName)
        qp.traitName = params.traitName;
    return gwasGet("/v2/associations", qp, opts);
}
//# sourceMappingURL=gwas_catalog.js.map