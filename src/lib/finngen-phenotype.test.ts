import {afterEach, expect, it, vi} from "vitest";
import {hashCanonicalJson} from "@bio-mcp/shared";
import {fetchFinnGenPhenotype, FINNGEN_JOIN_VERSION, loadFinnGenPhenotype} from "./finngen-phenotype";
import {readCompleteTable} from "./complete-table";
import {gatherInputs} from "./gather-inputs";
import {projectedSubrequests} from "./gather-budget";
vi.mock("./complete-table", () => ({readCompleteTable: vi.fn()}));
afterEach(() => {vi.resetAllMocks(); vi.unstubAllGlobals(); vi.useRealTimers();});
const native = {phenocode: "T2D", phenostring: "Synthetic selected phenotype", num_cases: 10, nested: {unchanged: [null, "α"]}};
const namespace = {} as DurableObjectNamespace;
const load = () => loadFinnGenPhenotype(namespace, "fixture", "R13", "T2D", FINNGEN_JOIN_VERSION);
async function observed() {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json(native)));
  return (await fetchFinnGenPhenotype("R13", "T2D"))[0];
}
it.each(["R12", "R13"] as const)("retains the exact %s definition and hash", async release => {
  const fetcher = vi.fn(async () => Response.json(native));vi.stubGlobal("fetch", fetcher);
  const [row] = await fetchFinnGenPhenotype(release, "T2D");
  expect(row).toMatchObject({status: "observed", requested_phenocode: "T2D", release_requested: release, response_sha256: await hashCanonicalJson(native)});
  expect(JSON.parse(row.native_response_json!)).toEqual(native);
  expect(fetcher).toHaveBeenCalledExactlyOnceWith(`https://${release.toLowerCase()}.finngen.fi/api/pheno/T2D`, expect.objectContaining({redirect: "manual"}));
});
it.each([404, 206, 302, 429])("retains native HTTP %i failure evidence", async status => {
  vi.stubGlobal("fetch", async () => Response.json(native, {status}));
  expect((await fetchFinnGenPhenotype("R13", "T2D"))[0]).toMatchObject({status: "error", http_status: status, native_response_json: JSON.stringify(native)});
});
it.each([{...native, phenocode: "OTHER"}, {...native, phenostring: " "}, null, [native]])("refuses a wrong or malformed native definition: %j", async value => {
  vi.stubGlobal("fetch", async () => Response.json(value));
  expect((await fetchFinnGenPhenotype("R13", "T2D"))[0].status).toBe("error");
});
it("rejects unsafe selections before IO and budgets the one definition request", async () => {
  const fetcher = vi.fn();vi.stubGlobal("fetch", fetcher);
  for (const code of ["", "../T2D", "T2D?query", "a".repeat(201)]) expect(() => gatherInputs({finngen_release: "R13", finngen_phenocode: code})).toThrow();
  expect(() => gatherInputs({finngen_phenocode: "T2D"})).toThrow("requires finngen_release");
  expect(fetcher).not.toHaveBeenCalled();
  const base = gatherInputs({finngen_release: "R13"});
  expect(projectedSubrequests({...base, finngen_phenocode: "T2D"}, 1) - projectedSubrequests(base, 1)).toBe(1);
});
it("rehashes a complete definition and returns a source reference without duplicating native JSON", async () => {
  vi.mocked(readCompleteTable).mockResolvedValue([await observed()]);
  const result = await load();expect(result.native).toEqual(native);
  expect(result.reference).toMatchObject({source_data_access_id: "fixture", source_table: "finngen_phenotype_evidence"});
  expect(result.reference).not.toHaveProperty("native_response_json");
});
it.each([{requested_phenocode: "OTHER"}, {release_requested: "R12"}, {source_url: "https://example.org"},
  {response_sha256: "wrong"}, {native_response_json: null}, {retrieved_at: "invalid"}, {http_status: 206}, {status: "error", error: null}])("rejects altered definition evidence: %j", async extra => {
  vi.mocked(readCompleteTable).mockResolvedValue([{...await observed(), ...extra}]);await expect(load()).rejects.toThrow();
});
it("rejects missing/duplicate definitions and unknown join versions", async () => {
  const row = await observed();
  for (const rows of [[], [row, row]]) {vi.mocked(readCompleteTable).mockResolvedValue(rows);await expect(load()).rejects.toThrow("Exactly one");}
  await expect(loadFinnGenPhenotype(namespace, "id", "R13", "T2D", "future")).rejects.toThrow("Unsupported");
});
it("keeps failed definition observations distinct from a negative association result", async () => {
  const row = {...await observed(), status: "error", error: "Transport failure", native_response_json: null, response_sha256: null, http_status: null};
  vi.mocked(readCompleteTable).mockResolvedValue([row]);expect(await load()).toMatchObject({native: null, reference: {status: "error", error: "Transport failure"}});
});
it("bounds oversized and stalled definitions", async () => {
  vi.stubGlobal("fetch", async () => Response.json({...native, extra: "α".repeat(140000)}));
  expect((await fetchFinnGenPhenotype("R13", "T2D"))[0]).toMatchObject({status: "error", native_response_json: null});
  vi.useFakeTimers();const cancel = vi.fn();
  vi.stubGlobal("fetch", async () => new Response(new ReadableStream({pull() {}, cancel})));
  const waiting = fetchFinnGenPhenotype("R13", "T2D");await vi.advanceTimersByTimeAsync(4001);
  expect((await waiting)[0].status).toBe("error");expect(cancel).toHaveBeenCalled();
});
