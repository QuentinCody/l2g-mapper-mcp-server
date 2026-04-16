import { describe, expect, it } from "vitest";
import { ensemblGet, ensemblPost } from "./ensembl.js";
function stub(json) {
    return (async () => new Response(JSON.stringify(json), { status: 200 }));
}
describe("ensembl adapter", () => {
    it("ensemblGet resolves JSON from GRCh38 by default", async () => {
        const captured = [];
        const f = async (input) => {
            captured.push(typeof input === "string" ? input : input.url);
            return new Response(JSON.stringify({ id: "rs7903146", mappings: [] }), { status: 200 });
        };
        const out = await ensemblGet("/variation/human/rs7903146", { fetchImpl: f });
        expect(out.id).toBe("rs7903146");
        expect(captured[0]).toContain("rest.ensembl.org");
    });
    it("ensemblPost sends JSON body", async () => {
        const out = await ensemblPost("/vep/human/region", { variants: ["1 100 . A G"] }, { fetchImpl: stub({ ok: true }) });
        expect(out).toEqual({ ok: true });
    });
});
//# sourceMappingURL=ensembl.test.js.map