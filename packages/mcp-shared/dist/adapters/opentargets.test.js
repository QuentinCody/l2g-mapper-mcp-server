import { describe, expect, it } from "vitest";
import { opentargetsGraphql } from "./opentargets.js";
describe("opentargets adapter", () => {
    it("posts a GraphQL query and returns the response data", async () => {
        const f = async () => new Response(JSON.stringify({ data: { search: { total: 1 } } }), { status: 200 });
        const result = await opentargetsGraphql("query Q { search { total } }", {}, { fetchImpl: f });
        expect(result.data?.search.total).toBe(1);
    });
});
//# sourceMappingURL=opentargets.test.js.map