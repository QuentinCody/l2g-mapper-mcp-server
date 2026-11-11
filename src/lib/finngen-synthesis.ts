import { hashCanonicalJson } from "@bio-mcp/shared";
import { readCompleteTable } from "./complete-table";
import { finnGenVariantId, type FinnGenObservation } from "./finngen-context";
import { loadFinnGenPhenotype } from "./finngen-phenotype";
import { finnGenVariantIdentity, joinFinnGenPhenotype } from "./finngen-join";

interface StagedAnchor {
  rsid: string; locus_id?: string | null; grch38_chr?: string | null; grch38_pos?: number | null;
  grch38_ref?: string | null; grch38_alt?: string | null;
}

function checkRequest(row: FinnGenObservation, anchor: StagedAnchor, release: string) {
  const variant = finnGenVariantId({ rsid: anchor.rsid, grch38: { chr: anchor.grch38_chr ?? "", pos: anchor.grch38_pos ?? 0,
    ref: anchor.grch38_ref ?? null, alt: anchor.grch38_alt ?? null } });
  const url = variant ? `https://${release.toLowerCase()}.finngen.fi/api/variant/${encodeURIComponent(variant)}` : null;
  if (row.release_requested !== release || row.assembly_requested !== "GRCh38" || row.release_basis !== "requested_host" ||
    row.locus_id !== (anchor.locus_id ?? null) || row.requested_variant !== variant || row.source_url !== url ||
    !["observed", "error", "skipped"].includes(row.status) || !Number.isFinite(Date.parse(row.retrieved_at))) {
    throw new Error("Staged FinnGen request does not match its anchor, release or source");
  }
  if (row.http_status !== null && (!Number.isInteger(row.http_status) || row.http_status < 100 || row.http_status > 599)) {
    throw new Error("Invalid staged FinnGen HTTP status");
  }
  if (row.status === "observed" ? row.error !== null || row.http_status !== 200 :
    typeof row.error !== "string" || !row.error) throw new Error("Staged FinnGen observation status mismatch");
  if (row.status === "skipped" ? variant !== null || row.http_status !== null || row.native_response_json !== null : variant === null) {
    throw new Error("Staged FinnGen skipped/requested state mismatch");
  }
}

async function checkNative(row: FinnGenObservation) {
  if (row.native_response_json === null) {
    if (row.status === "observed" || row.response_sha256 !== null) throw new Error("Staged FinnGen observation lacks native evidence");
    return { source_hash_verified: false, association_count: null, native: null };
  }
  if (typeof row.native_response_json !== "string" || row.http_status === null) throw new Error("Invalid staged FinnGen native evidence");
  const native: unknown = JSON.parse(row.native_response_json);
  if (await hashCanonicalJson(native) !== row.response_sha256) throw new Error("Staged FinnGen source hash mismatch");
  const associations = native && typeof native === "object" && !Array.isArray(native) && "results" in native && Array.isArray(native.results) ? native.results : null;
  if (row.status === "observed" && associations === null) throw new Error("Staged FinnGen observation lacks the native association array");
  return { source_hash_verified: true, association_count: row.status === "observed" ? associations!.length : null, native };
}

/** Recheck full native/request evidence; only bounded references enter the synthesis. */
export async function loadFinnGenContext(namespace: DurableObjectNamespace, id: string, capsJson: string | null | undefined, anchors: StagedAnchor[]) {
  const context = new Map<string, Record<string, unknown>[]>();
  const caps = JSON.parse(capsJson ?? "{}");
  const release = caps.finngen_release;
  if (caps.finngen_phenocode != null && release == null) throw new Error("FinnGen phenotype selection lacks a release");
  if (release === undefined || release === null) return context;
  if (release !== "R12" && release !== "R13") throw new Error("Unrecognized staged FinnGen release");
  const phenotype = caps.finngen_phenocode == null ? null : await loadFinnGenPhenotype(namespace, id, release, caps.finngen_phenocode, caps.finngen_join_version);
  const rows = await readCompleteTable<FinnGenObservation>(namespace, id, "finngen_evidence");
  const byRsid = new Map(anchors.map(anchor => [anchor.rsid, anchor]));
  if (rows.length > 25 || rows.length !== anchors.length || byRsid.size !== anchors.length) throw new Error("Incomplete or ambiguous staged FinnGen anchor set");
  for (const row of rows) {
    const anchor = byRsid.get(row.rsid);
    if (!anchor || context.has(row.rsid)) throw new Error("Staged FinnGen observation has an unknown or duplicate anchor");
    checkRequest(row, anchor, release);
    const {native, ...checked} = await checkNative(row);
    const identity = finnGenVariantIdentity(row.status === "observed" ? native : null, row.requested_variant);
    const join = phenotype ? joinFinnGenPhenotype(native, identity.verified, caps.finngen_phenocode, phenotype.native) : null;
    const { native_response_json: _native, ...reference } = row;
    context.set(row.rsid, [{ ...reference, ...checked, request_context_verified: true,
      source_data_access_id: id, source_table: "finngen_evidence", context_only: true,
      identity_verified: identity.verified, identity_basis: identity.reason,
      phenotype_join: join && {...join, definition_reference: phenotype!.reference,
        association_source: {source_data_access_id: id, source_table: "finngen_evidence", response_sha256: row.response_sha256, rsid: row.rsid}},
      trait_equivalence_verified: false, coverage_complete: false }]);
  }
  return context;
}
