import { afterEach, expect, it, vi } from "vitest";
import { observeGenebass, type GenebassObservation } from "./genebass-observation";
import { fetchGenebassContext } from "./genebass-context";
vi.mock("./genebass-observation", () => ({ observeGenebass: vi.fn() }));
afterEach(() => vi.resetAllMocks());

function observation(set: string): GenebassObservation {
  return { symbol: "GENE", requested_ensembl_id: "ENSG1", burden_set: set,
    source_url: "fixture", retrieved_at: "fixture", http_status: 200,
    native_response_json: '{"associations":[]}', response_sha256: "fixture", status: "observed", error: null,
    context_only: true, identity_verified: false, trait_matched: false, release_pinned: false, contract_verified: false };
}

it("preserves requested gene/set order when observations finish in reverse order", async () => {
  let resolveFirst!: (value: GenebassObservation) => void;
  vi.mocked(observeGenebass).mockReturnValueOnce(new Promise(resolve => { resolveFirst = resolve; }))
    .mockResolvedValueOnce(observation("missense|LC"));
  const pending = fetchGenebassContext(new Map([["GENE", "ENSG1"]]), ["pLoF", "missense|LC"], []);
  // Let the second observation complete while the first is still unresolved.
  await Promise.resolve(); await Promise.resolve();
  resolveFirst(observation("pLoF"));
  const result = await pending;
  expect(result.rows.map(row => row.burden_set)).toEqual(["pLoF", "missense|LC"]);
  expect(result.evidence.map(row => row.burden_set)).toEqual(["pLoF", "missense|LC"]);
});
