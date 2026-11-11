import { afterEach, expect, it, vi } from "vitest";
import { resolveAnchor } from "./coordinates";
import { gatherCoordinates } from "./gather-coordinates";
import { indexOpenTargetsAnchors } from "./opentargets-matching";

const native = (build: string, allele = "A/G") => ({ name: "rs1", synonyms: [], extra: "preserved", mappings: [{ assembly_name: build, coord_system: "chromosome", seq_region_name: "1", start: build === "GRCh38" ? 100 : 80, end: build === "GRCh38" ? 100 : 80, strand: 1, allele_string: allele }] });
afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers(); });

it("keeps assembly-specific native alleles paired with coordinates and records both requests", async () => {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const build = String(input).includes("grch37") ? "GRCh37" : "GRCh38";
    return Response.json(native(build, build === "GRCh38" ? "A/G" : "T/C"));
  }); vi.stubGlobal("fetch", fetchMock);
  const result = await resolveAnchor("rs1");
  expect(result).toMatchObject({ grch38: { chr: "1", pos: 100, ref: "A", alt: "G" }, grch37: { chr: "1", pos: 80, ref: "T", alt: "C" }, warnings: [] });
  expect(result.observations).toHaveLength(2);
  for (const row of result.observations) {
    expect(JSON.parse(row.native_response_json!)).toEqual(native(row.requested_assembly, row.requested_assembly === "GRCh38" ? "A/G" : "T/C"));
    expect(row).toMatchObject({ requested_rsid: "rs1", status: "resolved", retrieved_at: expect.any(String), error: null });
    expect(row.request_url).toContain("/variation/human/rs1?content-type=application/json");
  }
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

it("preserves multiallelic/strand context while excluding allele-specific queries", async () => {
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => Response.json(native(String(input).includes("grch37") ? "GRCh37" : "GRCh38", "A/G/T"))));
  const result = await resolveAnchor("rs1");
  expect(result.grch38).toEqual({ chr: "1", pos: 100, ref: null, alt: null });
  expect(result.warnings).toHaveLength(2);
  expect(indexOpenTargetsAnchors([{ rsid: "rs1", grch38: result.grch38 }], [])).toEqual(new Map());
  expect(JSON.parse(result.observations[0].native_response_json!).mappings[0].allele_string).toBe("A/G/T");
});

it("preserves one assembly when the other fails without borrowing its alleles", async () => {
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => String(input).includes("grch37") ? Response.json(native("GRCh37", "T/C")) : new Response("unavailable", { status: 503 })));
  const result = await resolveAnchor("rs1");
  expect(result.grch38).toBeNull(); expect(result.grch37?.alt).toBe("C");
  expect(result.observations[0]).toMatchObject({ status: "failed", native_response_json: null, selected_coord_json: "null", error: "Ensembl HTTP 503" });
  expect(result.warnings).toHaveLength(1);
});

it.each([() => new Response("x".repeat(1024 * 1024 + 1)), () => new Response("bad json"), () => new Response(null, { status: 302, headers: { Location: "https://elsewhere.example" } })])("does not accept oversized, malformed or redirected responses", async reply => {
  const fetchMock = vi.fn(async () => reply()); vi.stubGlobal("fetch", fetchMock);
  const result = await resolveAnchor("rs1");
  expect(result.grch38).toBeNull(); expect(result.grch37).toBeNull();
  expect(result.observations.every(row => row.status === "failed")).toBe(true);
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

it("rejects non-rsID inputs before any requests", async () => {
  const fetchMock = vi.fn(); vi.stubGlobal("fetch", fetchMock);
  expect((await resolveAnchor("rs1/other")).observations[0].error).toBe("Invalid rsID");
  expect(fetchMock).not.toHaveBeenCalled();
});

it("gathers native observations together with the qualified anchors", async () => {
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => Response.json(native(String(input).includes("grch37") ? "GRCh37" : "GRCh38"))));
  const anchors = [{ rsid: "rs1" }]; const warnings: string[] = [];
  const observations = await gatherCoordinates(anchors, warnings);
  expect(anchors).toMatchObject([{ grch38: { pos: 100 }, grch37: { pos: 80 } }]);
  expect(observations).toHaveLength(2); expect(warnings).toEqual([]);
});

it("does not publish late responses or warnings after the coordinate phase deadline", async () => {
  vi.useFakeTimers(); const resolvers: Array<(response: Response) => void> = [];
  // Deliberately ignore abort to model an upstream that settles after the deadline.
  vi.stubGlobal("fetch", vi.fn(() => new Promise<Response>(resolve => resolvers.push(resolve))));
  const anchors = [{ rsid: "rs1" }]; const warnings: string[] = [];
  const pending = gatherCoordinates(anchors, warnings);
  await vi.advanceTimersByTimeAsync(7001); expect(await pending).toEqual([]);
  expect(warnings).toEqual(["coordinate normalization exceeded 7000ms budget — skipped"]);
  for (const resolve of resolvers) resolve(Response.json(native("GRCh38")));
  await vi.advanceTimersByTimeAsync(1);
  expect(anchors).toEqual([{ rsid: "rs1" }]);
  expect(warnings).toHaveLength(1);
});

it("retains completed anchors when another anchor misses the phase deadline", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
    if (String(input).includes("/rs2?")) return new Promise<Response>(() => {});
    return Response.json(native(String(input).includes("grch37") ? "GRCh37" : "GRCh38"));
  }));
  const anchors = [{ rsid: "rs1" }, { rsid: "rs2" }]; const warnings: string[] = [];
  const pending = gatherCoordinates(anchors, warnings); await vi.advanceTimersByTimeAsync(7001);
  expect(await pending).toHaveLength(2);
  expect(anchors[0]).toMatchObject({ grch38: { ref: "A", alt: "G" } });
  expect(anchors[1]).toEqual({ rsid: "rs2" }); expect(warnings).toHaveLength(1);
});
