import type { VariantCoord } from "@bio-mcp/shared/variants/resolve";
import type { OtCsRow } from "./opentargets-contract";

export interface OpenTargetsAnchor { rsid: string; grch38?: VariantCoord | null }
export type OpenTargetsMatches = Map<string, { variantId: string; rsids: Set<string> }>;

/** Exact short-variant representation only; no liftover, strand flip or indel padding. */
export function openTargetsVariantId(coord?: VariantCoord | null): string | null {
  if (!coord || !Number.isSafeInteger(coord.pos) || coord.pos <= 0) return null;
  const chr = coord.chr.replace(/^chr/i, "").toUpperCase().replace(/^M$/, "MT");
  const ref = coord.ref?.toUpperCase(); const alt = coord.alt?.toUpperCase();
  if (!/^(?:[1-9]|1[0-9]|2[0-2]|X|Y|XY|MT)$/.test(chr)) return null;
  if (!ref || !alt || !/^[ACGT]+$/.test(ref) || !/^[ACGT]+$/.test(alt) || ref === alt) return null;
  return `${chr}_${coord.pos}_${ref}_${alt}`;
}

/** Snapshot resolved IDs before asynchronous collection can change anchor objects. */
export function indexOpenTargetsAnchors(anchors: OpenTargetsAnchor[], warnings: string[]) {
  const index = new Map<string, Set<string>>();
  for (const anchor of anchors) {
    const id = openTargetsVariantId(anchor.grch38);
    if (!id) { warnings.push(`OpenTargets ${anchor.rsid}: unambiguous GRCh38 sequence alleles unavailable; allele-specific evidence skipped`); continue; }
    const names = index.get(id) ?? new Set<string>();
    names.add(anchor.rsid.toLowerCase()); index.set(id, names);
  }
  return index;
}

export function matchOpenTargetsLoci(rows: OtCsRow[], index: Map<string, Set<string>>, warnings: string[]): OpenTargetsMatches {
  const seen = new Map<string, string | null>(); const conflicts = new Set<string>();
  const matches: OpenTargetsMatches = new Map();
  for (const row of rows) {
    const locus = row.studyLocusId; if (!locus) continue;
    const variantId = row.variant?.id ?? null;
    if (seen.has(locus) && seen.get(locus) !== variantId) conflicts.add(locus);
    seen.set(locus, variantId);
    const rsids = variantId ? index.get(variantId) : undefined;
    if (variantId && rsids) matches.set(locus, { variantId, rsids: new Set(rsids) });
  }
  for (const id of conflicts) {
    matches.delete(id); warnings.push(`OpenTargets ${id}: conflicting lead variant IDs in discovery; locus excluded`);
  }
  return matches;
}
