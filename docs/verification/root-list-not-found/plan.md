# Collection NotFound and filtered child discovery

## Scope and risk

Base: b00aa1a567ebc95caa50d1b6ee85b7caf5fa2b32.

Two audit candidates concern sync output: a root collection returning NotFound being treated as completed work, and a selected child resource type being undiscoverable without its unselected parent. Audit reports are hypotheses. Verify the SDK contracts and current behavior before changing either path. Do not introduce a dependency API merely to fit the second report.

Risk: HIGH if a change is required. Silent incomplete collection can be persisted into a successful artifact; recovery requires another sync. No resource, entitlement, grant, checkpoint, or artifact format changes are intended. No change to asymptotic collection or checkpoint cost is intended.

## Frozen behavioral criteria

| ID | Required behavior | Instrument and coverage | Initial status |
| -- | -- | -- | -- |
| NF-01 | A NotFound returned while listing a root collection must not be mistaken for proof that the collection is empty. | Real sync with deterministic connector fixture returning NotFound for root ListResources, both supported store/scheduler paths where applicable. Inspect returned error and artifact completion state. | Not assessed |
| NF-02 | A child listing for a parent that was deleted may remain a warning/no-op under the established SDK contract. | Paired parent-scoped fixture and existing warning tests; inspect emitted rows and completion. | Not assessed |
| NF-03 | A successful empty root collection remains valid. | Root ListResources returns an empty successful page; sync completes without fabricated resources. | Not assessed |
| NF-04 | Error classification preserves wrapped NotFound and does not reinterpret other codes. | Table over direct/wrapped NotFound, PermissionDenied, Unavailable and nil; root versus parent-scoped actions. | Not assessed |
| NF-05 | A root-collection failure does not become a successful sealed collection after retry/resume. | Real-store fixture inspecting failure and resumed collection, with fresh connector instance if changed state can persist. | Not assessed |
| NF-06 | Added regression assertions detect the old behavior. | Run the new tests against unchanged implementation, then after the minimal fix; retain exact output. | Not assessed |
| FL-01 | Determine whether filtered child discovery is a declared SDK dependency contract or a connector/filter configuration limitation. | Read the public filter and child-discovery contract, current annotations, tests and actual caller; implement a dedicated reproduction before claiming a defect. | Not assessed |

## Review and delivery

After implementation inspection, append implementation obligations without silently rewriting this baseline. Every applicable changed branch needs an instrument or an explicit gap. Run an independent evidence audit and focused implementation review before PR submission. Run repository-required checks; name failures and unavailable checks. Tests use temporary stores and in-process connectors, not shared services or vendor APIs. Do not claim live-provider or production behavior from fixture results. Refuted or intentionally unsupported filter behavior is recorded as excluded, not turned into speculative production code.
