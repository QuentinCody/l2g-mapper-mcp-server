import { expect, it } from "vitest";
import { validateGraphqlQuery } from "@bio-mcp/shared/codemode/graphql-validate";
import type { TrimmedField, TrimmedIntrospection } from "@bio-mcp/shared/codemode/graphql-introspection";
import { workflowSchema } from "../../../open-targets-mcp-server/src/lib/workflow-schema.fixture";
import { OT_SEARCH_STUDY_QUERY, OT_STUDY_CS_QUERY, OT_BATCH_DETAIL_QUERY } from "./opentargets-contract";
import { OT_BURDEN_QUERY } from "./burden-evidence";

// Extend the independently transcribed provider snapshot with fields read from
// https://api.platform.opentargets.org/api/v4/graphql/schema on September 30, 2026.
const field = (name: string, type: string, args: Record<string, string> = {}): TrimmedField => ({ name, type, args: Object.entries(args).map(([name, type]) => ({ name, type })) });
const additions: Record<string, TrimmedField[]> = {
  Evidence: [field("diseaseFromSource", "String"), field("diseaseFromSourceId", "String"), field("diseaseFromSourceMappedId", "String"), field("studyOverview", "String"), field("cohortDescription", "String"), field("studyCases", "Long"), field("studyCasesWithQualifyingVariants", "Long"), field("allelicRequirements", "[String!]")],
  Query: [field("search", "SearchResults!", { queryString: "String!", entityNames: "[String!]", page: "Pagination" }), field("study", "Study", { studyId: "String!" }), field("credibleSets", "CredibleSets!", { studyLocusIds: "[String!]", page: "Pagination" })],
  Study: [field("hasSumstats", "Boolean"), field("traitFromSourceMappedIds", "[String!]"), field("nCases", "Int"), field("nControls", "Int"), field("cohorts", "[String!]"), field("discoverySamples", "[Sample!]"), field("replicationSamples", "[Sample!]"), field("credibleSets", "CredibleSets!", { page: "Pagination" })],
  Variant: [field("rsIds", "[String!]")],
  SearchResults: [field("total", "Long!"), field("hits", "[SearchResult!]!")],
  SearchResult: [field("score", "Float!"), field("object", "EntityUnionType")],
  Sample: [field("ancestry", "String!"), field("sampleSize", "Int!")],
};
const schema: TrimmedIntrospection = {
  ...workflowSchema,
  types: [
    ...workflowSchema.types.map(type => ({ ...type, fields: [...(type.fields ?? []), ...(additions[type.name] ?? [])] })),
    ...Object.entries(additions).filter(([name]) => !workflowSchema.types.some(type => type.name === name)).map(([name, fields]) => ({ name, kind: "OBJECT", fields })),
    { name: "EntityUnionType", kind: "UNION", possibleTypes: [{ name: "Study" }] },
  ],
};
it.each([OT_STUDY_CS_QUERY, OT_BATCH_DETAIL_QUERY, OT_BURDEN_QUERY])("matches provider fields and argument names %#", query => {
  expect(validateGraphqlQuery(query, schema)).toEqual({ checked: true, errors: [] });
});
it("checks the search Study branch separately because the shared validator cannot parse fragments", () => {
  expect(validateGraphqlQuery(OT_SEARCH_STUDY_QUERY, schema)).toEqual({ checked: false, errors: [] });
  const fragment = OT_SEARCH_STUDY_QUERY.match(/\.\.\. on Study \{([^{}]+)\}/)!;
  expect(fragment).not.toBeNull();
  const narrowed = { ...schema, types: schema.types.map(type => type.name === "SearchResult" ? { ...type, fields: type.fields!.map(value => value.name === "object" ? { ...value, type: "Study" } : value) } : type) };
  expect(validateGraphqlQuery(OT_SEARCH_STUDY_QUERY.replace(fragment[0], fragment[1]), narrowed)).toEqual({ checked: true, errors: [] });
});
it("detects a renamed prediction field rather than accepting an empty lane", () => {
  expect(validateGraphqlQuery(OT_BATCH_DETAIL_QUERY.replace("l2GPredictions", "studyLocus2GeneTable"), schema).errors.length).toBeGreaterThan(0);
});
