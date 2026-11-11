import { observeGenebass, type GenebassObservation } from "./genebass-observation";
import { projectGenebassPhewas } from "./genebass-native";

export interface BurdenRow {
	symbol: string;
	ensembl_id: string;
	burden_set: string;
	phenotype: string | null;
	phenotype_id?: string | null;
	p_value: number | null;
	support: "strong" | "nominal" | "none";
}

export function bestGenebassAssociation(data: unknown) {
	if (!data || typeof data !== "object") {
		throw new Error("Unrecognized Genebass response: expected an associations array");
	}
	if (("error" in data && data.error) || ("errors" in data && (!Array.isArray(data.errors) || data.errors.length))) {
		throw new Error("Genebass response contains upstream errors");
	}
	const current = "phewas" in data;
	const associations = current ? projectGenebassPhewas(data as Record<string, unknown>) : "associations" in data ? data.associations : null;
	if (!Array.isArray(associations)) throw new Error("Unrecognized Genebass response: expected a phewas or associations array");
	let pValue: number | null = null;
	let phenotype: string | null = null;
	let phenotypeId: string | null = null;
	let associationIndex: number | null = null;
	for (const [index, row] of associations.entries()) {
		if (!row || typeof row !== "object" || !("skat_o_pvalue" in row)) throw new Error("Malformed Genebass association");
		const p = row.skat_o_pvalue;
		if (p === null || p === undefined) continue;
		if (typeof p !== "number" || !Number.isFinite(p) || p <= 0 || p > 1) {
			throw new Error("Invalid Genebass SKAT-O p-value; expected a finite number in (0, 1]");
		}
		if (row.phenotype_description != null && typeof row.phenotype_description !== "string") {
			throw new Error("Malformed Genebass phenotype description");
		}
		if (pValue === null || p < pValue) {
			pValue = p;
			phenotype = row.phenotype_description ?? null;
			phenotypeId = row.phenotype_id ?? null;
			associationIndex = index;
		}
	}
	return current ? { pValue, phenotype, associationIndex, phenotypeId, associationArray: "phewas" } : { pValue, phenotype, associationIndex };
}

export async function fetchGenebassContext(
	symbolToEnsembl: Map<string, string>,
	burdenSets: string[],
	warnings: string[],
): Promise<{ rows: BurdenRow[]; evidence: GenebassObservation[] }> {
	const entries = [...symbolToEnsembl.entries()].slice(0, 25); // hard cap for subrequest budget
	const sets = [...new Set(burdenSets)];
	if (sets.length > 4) throw new Error("Genebass supports at most 4 requested burden sets per gather");
	if (sets.length === 0) return { rows: [], evidence: [] };
	if (symbolToEnsembl.size > entries.length) warnings.push("Genebass context is limited to the first 25 candidate genes");
	const jobs: (() => Promise<{ row?: BurdenRow; evidence: GenebassObservation }>)[] = [];
	scheduleGenebassReads(entries, sets, jobs, warnings);
	const results: { row?: BurdenRow; evidence: GenebassObservation }[] = [];
	// At most two 2 MiB responses in flight; reserve room for both before the next batch.
	for (let index = 0; index < jobs.length; index += 2) {
		results.push(...await Promise.all(jobs.slice(index, index + 2).map(job => job())));
	}
	return { rows: results.flatMap(result => result.row ? [result.row] : []), evidence: results.map(result => result.evidence) };
}

function scheduleGenebassReads(entries: [string, string][], sets: string[], jobs: (() => Promise<{ row?: BurdenRow; evidence: GenebassObservation; }>)[], warnings: string[]) {
  const deadline = Date.now() + 11000;
  let retainedBytes = 0;
  for (const [symbol, ensembl] of entries) {
    for (const burdenSet of sets) {
      jobs.push(
        (async () => {
          const skip = Date.now() + 4000 >= deadline ? "Genebass lane deadline; request not attempted"
            : retainedBytes > 4 * 1024 * 1024 ? "Genebass 8 MiB evidence budget; request not attempted" : undefined;
          const observation = await observeGenebass(symbol, ensembl, burdenSet, skip);
          retainedBytes += new TextEncoder().encode(observation.native_response_json ?? "").byteLength;
          try {
            if (observation.status === "error") throw new Error(observation.error ?? "Genebass request failed");
            const summary = bestGenebassAssociation(JSON.parse(observation.native_response_json!));
            const { pValue: best_p, phenotype: best_phenotype } = summary;
            let support: BurdenRow["support"] = "none";
            if (best_p != null && best_p < 2.5e-6) support = "strong";
            else if (best_p != null && best_p < 0.05) support = "nominal";
            return {
              evidence: observation, row: {
                symbol,
                ensembl_id: ensembl,
                burden_set: burdenSet,
                phenotype: best_phenotype,
                ...("phenotypeId" in summary ? { phenotype_id: summary.phenotypeId } : {}),
                p_value: best_p,
                support,
              }
            };
          } catch (err) {
            observation.status = "error";
            observation.error = err instanceof Error ? err.message.slice(0, 500) : "Invalid Genebass response";
            warnings.push(`Genebass failed for ${symbol}/${burdenSet}: ${err instanceof Error ? err.message : String(err)}`);
            return { evidence: observation };
          }
        })
      );
    }
  }
}
