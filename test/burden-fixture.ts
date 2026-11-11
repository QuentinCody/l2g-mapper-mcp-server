import { hashCanonicalJson } from "@bio-mcp/shared";
import { OT_BURDEN_QUERY } from "../src/lib/burden-evidence";

export function nativeBurden(count = 1) {
  return { data: { meta: { dataVersion: { year: "2026", month: "09", iteration: "0" } },
    associationDatasources: [{ datasource: "gene_burden", datatype: "genetic_association" }],
    disease: { id: "EFO_1", name: "synthetic trait", evidences: { count, cursor: null,
      rows: Array.from({ length: count }, (_, index) => ({ id: `e${index}`, datasourceId: "gene_burden", datatypeId: "genetic_association",
        target: { id: "ENSG1", approvedSymbol: "GENE" }, disease: { id: "EFO_1" }, diseaseFromSourceMappedId: "EFO_1",
        diseaseFromSourceId: "source-phenotype", studyId: "synthetic-study", statisticalMethod: "SKAT-O",
        pValueMantissa: 2, pValueExponent: -8, qualityControls: [], warningMessage: null, literature: ["synthetic-publication"],
        studyOverview: "α".repeat(300),
      })),
    } },
  } };
}

export async function burdenObservation(native: unknown = nativeBurden()) {
  const data = native as ReturnType<typeof nativeBurden>;
  return { phase: "gene burden", source_url: "https://api.platform.opentargets.org/api/v4/graphql",
    query: OT_BURDEN_QUERY, variables_json: JSON.stringify({ genes: ["ENSG1"], trait: "EFO_1" }),
    response_json: JSON.stringify(native), response_sha256: await hashCanonicalJson(native),
    retrieved_at: "2026-10-01T00:00:00Z", data_version_json: JSON.stringify(data.data.meta.dataVersion),
    usable: true, error: null,
  };
}
