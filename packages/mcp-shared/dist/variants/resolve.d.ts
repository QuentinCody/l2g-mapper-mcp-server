/**
 * Variant identifier resolution and cohort-format helpers.
 *
 * Ports the OpenAI life-science plugin's `variant_resolution.py` (shared across
 * all 5 PheWAS skills) into TypeScript. Used by `phewas-mcp-server` and
 * `l2g-mapper-mcp-server`.
 */
export type GenomeBuild = "GRCh37" | "GRCh38";
export interface VariantCoord {
    chr: string;
    pos: number;
    ref: string | null;
    alt: string | null;
    canonical?: string;
}
export interface ResolvedVariant {
    input: {
        type: "rsid" | "grch37" | "grch38";
        value: string;
    };
    rsid: string | null;
    grch37: VariantCoord | null;
    grch38: VariantCoord | null;
    warnings: string[];
}
export interface ParsedVariant {
    chr: string;
    pos: number;
    ref: string;
    alt: string;
}
export declare class VariantResolutionError extends Error {
    code: string;
    warnings: string[];
    constructor(code: string, message: string, warnings?: string[]);
}
/** "GRCh37" -> "grch37", everything else -> "grch38". */
export declare function buildKeyFor(build: GenomeBuild | "hg19"): "grch37" | "grch38";
/** Parses a free-form variant string like `chr1-123-A-G`, `1:123_A/G`, etc. */
export declare function parseVariantString(value: string): ParsedVariant;
/** Convenience wrapper exposed in the plan's API. */
export declare function parseVariant(input: string): ParsedVariant;
export declare function buildVariantRecord(chrom: string, pos: number, ref: string | null, alt: string | null): VariantCoord;
interface RsidLookupResult {
    chr: string;
    pos: number;
    ref: string | null;
    alts: string[];
}
interface PositionLookupResult {
    rsid: string;
    ref: string | null;
    alts: string[];
}
export interface ResolveOptions {
    fetchImpl?: typeof fetch;
    timeoutMs?: number;
}
export declare function lookupRsid(rsid: string, build?: GenomeBuild, opts?: ResolveOptions): Promise<RsidLookupResult | null>;
export declare function lookupPosition(chrom: string, pos: number, build?: GenomeBuild, opts?: ResolveOptions): Promise<PositionLookupResult | null>;
interface BothBuildsResult {
    rsid: string;
    grch38: {
        chr: string | null;
        pos: number | null;
    };
    grch37: {
        chr: string | null;
        pos: number | null;
    };
    ref: string | null;
    alts: string[];
    warnings: string[];
}
export declare function resolveRsidBothBuilds(rsid: string, opts?: ResolveOptions): Promise<BothBuildsResult>;
/** Resolves a variant given as an rsID. Returns coords for both builds. */
export declare function resolveRsid(rsid: string, opts?: ResolveOptions): Promise<ResolvedVariant>;
/**
 * Lift a variant between builds via Ensembl: position lookup on the source
 * build to find an rsID, then rsID lookup on the target build.
 */
export declare function liftover(variant: ParsedVariant, fromBuild: GenomeBuild, toBuild: GenomeBuild, opts?: ResolveOptions): Promise<VariantCoord>;
export declare function resolveVariant(inputType: "rsid" | "grch37" | "grch38", inputValue: string, opts?: ResolveOptions): Promise<ResolvedVariant>;
/**
 * Cohort identifiers supported by phewas-mcp-server. The format string is the
 * URL path-segment expected by the cohort's `/api/variant/{...}` endpoint.
 *
 * Notes:
 *   - finngen / ukb-topmed / tpmi expect GRCh38 coords
 *   - bbj expects GRCh37 coords
 *   - genebass uses gene-level burden (separate path; see formatGenebass)
 *
 * All cohorts use the canonical `chrom:pos-ref-alt` format string.
 */
export type Cohort = "finngen" | "ukb-topmed" | "bbj" | "tpmi";
export declare const COHORT_BUILD: Record<Cohort, GenomeBuild>;
/**
 * Produce the cohort-specific URL path-segment for `/api/variant/{...}`.
 * The caller is responsible for liftover if the input variant is in a
 * different build than the cohort expects (use `liftover()` first or
 * `resolveVariant()` and pick the right `grch37`/`grch38` field).
 */
export declare function formatForCohort(variant: VariantCoord, _cohort: Cohort): string;
export {};
//# sourceMappingURL=resolve.d.ts.map