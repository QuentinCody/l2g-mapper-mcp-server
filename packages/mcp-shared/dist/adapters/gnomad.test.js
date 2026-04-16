import { describe, expect, it } from "vitest";
import { gnomadGraphql } from "./gnomad.js";
describe("gnomad adapter", () => {
    it("posts GraphQL query and returns data", async () => {
        const f = async () => new Response(JSON.stringify({ data: { gene: { gene_id: "ENSG00000148737" } } }), { status: 200 });
        const r = await gnomadGraphql("query { gene { gene_id } }", {}, { fetchImpl: f });
        expect(r.data?.gene.gene_id).toBe("ENSG00000148737");
    });
});
//# sourceMappingURL=gnomad.test.js.map