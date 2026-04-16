import { describe, expect, it } from "vitest";
import { genebassGeneBurden } from "./genebass.js";
describe("genebass adapter", () => {
    it("hits gene_burden_phewas with expected query params", async () => {
        const captured = [];
        const f = async (input) => {
            captured.push(typeof input === "string" ? input : input.url);
            return new Response(JSON.stringify({ ok: true, associations: [] }), { status: 200 });
        };
        const result = await genebassGeneBurden("ENSG00000147883", "pLoF", { fetchImpl: f });
        expect(captured[0]).toContain("gene_id=ENSG00000147883");
        expect(captured[0]).toContain("burden_set=pLoF");
        expect(result).toMatchObject({ ok: true });
    });
});
//# sourceMappingURL=genebass.test.js.map