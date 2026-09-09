# Encrypted Action Results Verification Plan

Status: frozen implementation-blind baseline.

## Risk

This change is HIGH risk. A plaintext action result can cross an RPC boundary,
enter task storage, and be logged. Remediation cannot retract a disclosed secret,
and the public protobuf and Go APIs have consumers outside this repository.

The feature is opt-in. Existing `ActionHandler` registrations and actions without
secret return values retain their current behavior.

## Contract

- C1 supplies encryption recipients on `ActionInvokeTask`; the task handler copies
  them to `InvokeActionRequest`.
- A new handler type returns public result fields separately from
  `PlaintextData`.
- A secret handler cannot run unless at least one valid encryption recipient was
  supplied.
- Every plaintext result name identifies exactly one `return_types` field marked
  `is_secret`; duplicate, empty, undeclared, and non-secret names are rejected.
- A public result struct must not contain a field declared secret.
- Encryption runs once when the handler settles. `OutstandingAction` retains only
  ciphertext, never plaintext.
- A completed inline response and every later status response return the same
  encrypted values. Pending and running responses contain no encrypted values.
- One plaintext value and N recipients produce N `EncryptedData` values. Encryption
  failure settles the action as failed without exposing plaintext.
- Existing handlers, deprecated action managers, and actions with only public
  return fields require no encryption configuration and preserve their API behavior.
- `EncryptedData.name` equals the matching return field name.

## Coverage model

The executable dimensions are:

- handler: existing / secret;
- scope: global / resource;
- observation: completed inline / pending then status;
- recipients: missing / invalid / one / multiple;
- output: no plaintext / one plaintext / duplicate names / empty name or bytes /
  undeclared name / name declared non-secret / secret name also present publicly;
- handler result: success / error / panic / cancellation.

Global and resource registration share the same result adapter and settlement
function; representative tests may reduce output-validation and encryption-failure
cells across scope after implementation inspection confirms that path.

## Criteria

- C1: Generated protobufs expose encryption configs on action invocation tasks and
  requests, and encrypted data on invoke and status responses.
- C2: Existing handler behavior and source compatibility remain intact.
- C3: Secret handlers reject missing or invalid recipients before provider work.
- C4: Secret output validation rejects every invalid output class without returning
  plaintext.
- C5: Encryption fan-out preserves metadata and creates one ciphertext per
  plaintext-recipient pair.
- C6: Inline completion and status polling expose identical ciphertext.
- C7: Concurrent status reads cannot race with settlement or observe a partial
  outcome.
- C8: Logs and RPC messages contain no plaintext result.
- C9: Existing actions incur no asymptotic cost change; secret actions add
  O(P × R) encryption work for P plaintext values and R recipients.

## Instruments

- Proto generation and Buf lint/breaking checks cover C1.
- Table-driven package tests cover C2-C6 and the output-validation rows.
- Race detection on `pkg/actions` and `pkg/connectorbuilder` covers C7.
- Tests assert plaintext absence from public structs and protobuf responses for C8.
- Code inspection confirms the cost expression and that existing handlers bypass
  encryption work for C9.

Evidence and criterion status are recorded in `evidence.md`.

## Change orders

### CO-001 — compatibility and local invocation

Source: independent implementation audits.

Classification:

- Correction: keep the exported `connectorbuilder.ActionManager` method set
  unchanged. Encrypted dispatch is an internal optional interface implemented by
  the SDK action manager.
- Correction: redact recovered panic values from logs and action errors because a
  connector can panic with secret-bearing data.
- Extension: add local action-invocation constructors and runner options that
  accept encryption recipients and retain the existing constructors as nil-recipient
  delegates.

Affected criteria: C2, C7, C8. Verification adds compilation of existing action
consumers, a panic-redaction test, local task recipient plumbing, and another focused
race run. The extension does not change the protobuf contract or secret settlement
path.
