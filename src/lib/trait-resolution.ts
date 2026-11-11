import { ols4Search } from "@bio-mcp/shared/adapters/efo_ols4";
import { boundedJsonFetch } from "./bounded-json-fetch";
import { traitId } from "./opentargets-traits";

export interface TraitResolution {
  query: string | null;
  efo_id: string | null;
  label: string | null;
  status: "explicit_id" | "exact_label" | "ambiguous" | "unmatched" | "incomplete" | "failed" | "not_requested";
  retrieved_at: string | null;
  native_response_json: string | null;
}

const normalizedLabel = (value: unknown) => typeof value === "string" ? value.trim().replace(/\s+/g, " ").toLowerCase() : "";
const record = (value: unknown): Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};

/** Search rank and synonym hits do not establish a selected phenotype. */
export function selectExactTrait(query: string, native: unknown): Pick<TraitResolution, "efo_id" | "label" | "status"> {
  const response = record(record(native).response);
  const docs = response.docs;
  const empty = { efo_id: null, label: null };
  if (!Array.isArray(docs) || !Number.isSafeInteger(response.numFound) || response.numFound !== docs.length ||
      response.numFoundExact === false || (response.start !== undefined && response.start !== 0)) {
    return { ...empty, status: "incomplete" };
  }
  const matches = docs.map(record).filter(doc => normalizedLabel(query) !== "" && normalizedLabel(doc.label) === normalizedLabel(query));
  if (docs.some(doc => !normalizedLabel(record(doc).label))) return { ...empty, status: "incomplete" };
  if (!matches.length) return { ...empty, status: "unmatched" };
  const identities = matches.map(doc => traitId(doc.obo_id));
  if (identities.some(id => id === null) || matches.some(doc => doc.type !== "class" || doc.ontology_name !== "efo" || doc.is_obsolete === true)) {
    return { ...empty, status: "incomplete" };
  }
  if (new Set(identities).size !== 1) return { ...empty, status: "ambiguous" };
  return { efo_id: identities[0], label: String(matches[0].label), status: "exact_label" };
}

export async function resolveTrait(query: string | null, explicitId: string | null, warnings: string[]): Promise<TraitResolution> {
  const base = { query, efo_id: explicitId, label: null, retrieved_at: null, native_response_json: null };
  if (explicitId) {
    if (!traitId(explicitId)) throw new Error("efo_id must be an ontology identifier such as EFO_0001360 or EFO:0001360");
    return { ...base, efo_id: traitId(explicitId), status: "explicit_id" };
  }
  if (!query) return { ...base, status: "not_requested" };
  try {
    const native = await ols4Search(query, { exact: true, rows: 25, timeoutMs: 7000, fetchImpl: boundedJsonFetch("OLS4", 1024 * 1024, 7000) });
    const selected = selectExactTrait(query, native);
    if (selected.status !== "exact_label") warnings.push(`OLS4 trait resolution ${selected.status}: provide an explicit efo_id after inspecting trait_resolution; search rank and synonyms are not phenotype identity.`);
    return { ...base, ...selected, retrieved_at: new Date().toISOString(), native_response_json: JSON.stringify(native) };
  } catch (error) {
    warnings.push(`OLS4 trait resolution failed: ${error instanceof Error ? error.message : String(error)}`);
    return { ...base, status: "failed" };
  }
}
