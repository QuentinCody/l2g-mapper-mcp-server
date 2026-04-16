/**
 * Ensembl REST API adapter.
 * Covers variation, overlap, VEP, and region endpoints commonly used across
 * bio-mcp servers. Promoted from per-server `lib/api-adapter.ts` per the
 * life-science expansion plan §4.3 (Phase 3C).
 */
const ENSEMBL_GRCH38 = "https://rest.ensembl.org";
const ENSEMBL_GRCH37 = "https://grch37.rest.ensembl.org";
function baseFor(build) {
    return build === "GRCh37" ? ENSEMBL_GRCH37 : ENSEMBL_GRCH38;
}
export async function ensemblGet(path, opts = {}) {
    const { fetchImpl = fetch, timeoutMs = 15_000, build, userAgent = "bio-mcp-ensembl/1.0" } = opts;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
        const url = path.startsWith("http") ? path : `${baseFor(build)}${path}`;
        const resp = await fetchImpl(url, {
            headers: { Accept: "application/json", "User-Agent": userAgent },
            signal: ctrl.signal,
        });
        if (!resp.ok)
            throw new Error(`Ensembl HTTP ${resp.status}: ${await resp.text().catch(() => "")}`);
        return (await resp.json());
    }
    finally {
        clearTimeout(timer);
    }
}
export async function ensemblPost(path, body, opts = {}) {
    const { fetchImpl = fetch, timeoutMs = 20_000, build, userAgent = "bio-mcp-ensembl/1.0" } = opts;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
        const url = path.startsWith("http") ? path : `${baseFor(build)}${path}`;
        const resp = await fetchImpl(url, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                Accept: "application/json",
                "User-Agent": userAgent,
            },
            body: JSON.stringify(body),
            signal: ctrl.signal,
        });
        if (!resp.ok)
            throw new Error(`Ensembl HTTP ${resp.status}: ${await resp.text().catch(() => "")}`);
        return (await resp.json());
    }
    finally {
        clearTimeout(timer);
    }
}
//# sourceMappingURL=ensembl.js.map