import { hashCanonicalJson } from "@bio-mcp/shared";
import { fetchDatasets } from "@bio-mcp/shared/adapters/eqtl/ftp-catalogue";
import { assertR7Snapshot } from "@bio-mcp/shared/adapters/eqtl/r7-snapshot";
import { createIndexedFileReader } from "@bio-mcp/shared/adapters/eqtl/indexed-file";
import { queryRegion, type RegionResult } from "@bio-mcp/shared/adapters/eqtl/sumstats";
import { eqtlCatalogueSchema, type EqtlCatalogueInput } from "./eqtl-catalogue-input";
import { eqtlCatalogueTransport } from "./eqtl-catalogue-transport";
import { fetchEqtlFineMapping } from "./eqtl-fine-mapping";

interface Anchor {
  rsid: string; locus_id?: string;
  grch38?: { chr: string; pos: number; ref: string | null; alt: string | null } | null;
}
export interface EqtlCatalogueObservation {
  rsid: string; locus_id: string | null; requested_variant: string | null;
  requested_dataset_id: string; release_requested: "r7"; assembly_requested: "GRCh38";
  metadata_sha256_requested: string; paths_sha256_requested: string;
  source_url: string | null; retrieved_at: string;
  native_response_json: string | null; response_sha256: string | null;
  status: "observed" | "error" | "skipped"; error: string | null;
  allele_matching_row_count: number; context_only: true; coverage_complete: false;
}

export function eqtlAnchorVariant(anchor: Anchor): string | null {
  const c = anchor.grch38;
  if (!c || !/^(?:[1-9]|1\d|2[0-2]|X|Y)$/.test(c.chr) || !Number.isSafeInteger(c.pos) || c.pos < 1 ||
    !c.ref || !c.alt || !/^[ACGT]+$/.test(c.ref) || !/^[ACGT]+$/.test(c.alt) || c.ref === c.alt) return null;
  return `${c.chr}_${c.pos}_${c.ref}_${c.alt}`;
}

/** Exact native alleles only; no rsID fallback, strand flipping or effect transformation. */
export function matchingEqtlRows(region: RegionResult, variant: string): number[] {
  const [chr, pos, ref, alt] = variant.split("_");
  return region.rows.flatMap((row, i) => typeof row.variant === "string" && row.variant.replace(/^chr/, "") === variant &&
    String(row.chromosome).replace(/^chr/, "") === chr && row.position === Number(pos) && row.ref === ref && row.alt === alt ? [i] : []);
}

function initial(input: EqtlCatalogueInput, anchor?: Anchor): EqtlCatalogueObservation {
  return { rsid: input.rsid, locus_id: anchor?.locus_id ?? null, requested_variant: anchor ? eqtlAnchorVariant(anchor) : null,
    requested_dataset_id: input.dataset_id, release_requested: "r7", assembly_requested: "GRCh38",
    metadata_sha256_requested: input.metadata_sha256, paths_sha256_requested: input.paths_sha256,
    source_url: null, retrieved_at: new Date().toISOString(), native_response_json: null, response_sha256: null,
    status: "skipped", error: null, allele_matching_row_count: 0, context_only: true, coverage_complete: false };
}

/** One point query in one explicitly chosen r7 analysis; retains all returned native rows. */
export async function fetchEqtlCatalogueContext(anchors: Anchor[], requested: EqtlCatalogueInput | undefined, warnings: string[]) {
  if (!requested) return [];
  const input = eqtlCatalogueSchema.parse(requested);
  const selected = anchors.filter(anchor => anchor.rsid === input.rsid);
  const anchor = selected.length === 1 ? selected[0] : undefined;
  const observation = initial(input, anchor);
  if (!observation.requested_variant || !anchor?.grch38) {
    observation.error = "Selected rsID is absent, ambiguous, or lacks an exact biallelic GRCh38 anchor";
    warnings.push(`eQTL Catalogue ${input.rsid}: ${observation.error}`);
    return [observation];
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error("eQTL Catalogue lane exceeded 15 seconds")), 15_000);
  return await observeCatalogueRegion(controller, input, observation, { coordinates: anchor.grch38, variant: observation.requested_variant }, warnings, timer);
}

async function observeCatalogueRegion(controller: AbortController, input: { release: "r7"; dataset_id: string; rsid: string; metadata_sha256: string; paths_sha256: string; max_rows: number; fine_mapping_sha256?: string | undefined; }, observation: EqtlCatalogueObservation, selected: { coordinates: NonNullable<Anchor["grch38"]>; variant: string }, warnings: string[], timer: ReturnType<typeof setTimeout>) {
  try {
    const io = eqtlCatalogueTransport(controller.signal);
    const datasets = await fetchDatasets(io.resourcesFetch);
    controller.signal.throwIfAborted();
    const dataset = datasets.find(row => row.dataset_id === input.dataset_id);
    if (!dataset) throw new Error("Requested r7 dataset is absent from the catalogue");
    assertR7Snapshot(dataset.catalogue_snapshot, input);
    observation.source_url = dataset.sumstats_url;
    const region = await queryRegion({
      dataset, chromosome: selected.coordinates.chr, start: selected.coordinates.pos,
      end: selected.coordinates.pos, maxRows: input.max_rows
    }, { ...createIndexedFileReader(io.fetchFile), fetchFile: io.fetchFile });
    controller.signal.throwIfAborted();
    const fineMapping = await fetchEqtlFineMapping(dataset, selected.variant, input, io, controller.signal);
    const native = { dataset, region, ...(fineMapping ? { fine_mapping: fineMapping } : {}) };
    if (fineMapping?.error) warnings.push(`eQTL Catalogue fine mapping: ${fineMapping.error}; nominal context retained without Catalogue score support`);
    const digest = await hashCanonicalJson(native);
    observation.native_response_json = JSON.stringify(native);
    observation.response_sha256 = digest;
    observation.allele_matching_row_count = matchingEqtlRows(region, selected.variant).length;
    observation.status = "observed";
  } catch (error) {
    observation.status = "error";
    observation.error = error instanceof Error ? error.message.slice(0, 500) : "eQTL Catalogue read failed";
    warnings.push(`eQTL Catalogue ${input.dataset_id} ${input.rsid}: ${observation.error}`);
  } finally { clearTimeout(timer); controller.abort(); observation.retrieved_at = new Date().toISOString(); }
  warnings.push("eQTL Catalogue nominal associations alone do not contribute to scores. Explicit checksum-pinned fine mapping requires exact native joins and PIP >= 0.95. This is not colocalisation or independent GTEx replication. ALT is the effect allele; native duplicate rows are retained and coverage is incomplete.");
  return [observation];
}
