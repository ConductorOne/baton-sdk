# Encrypted Action Results Evidence

Revision: working tree.

## Current-stage criteria

- C1: verified to stated coverage by generation, Buf lint, Buf breaking, and
  `TestActionInvokeTaskThreadsEncryptionConfigs`.
- C2: verified to stated coverage by the existing action and connectorbuilder
  suites plus secret-registration rejection tests.
- C3: verified to stated coverage for missing, nil, and unknown-provider
  recipients; each case asserts the handler was not invoked.
- C4: verified to stated coverage for public secret fields, undeclared and
  non-secret names, duplicate names, nil values, empty names, empty bytes, handler
  errors, and injected encryption failure.
- C5: verified to stated coverage for two plaintext values with two age recipients,
  including name, description, schema, decrypted bytes, and the full P × R fan-out.
- C6: verified to stated coverage by inline and pending-then-status cases. The
  status test also mutates the caller-owned recipient after invoke and proves
  settlement uses the captured recipient.
- C7: verified to stated coverage by focused race tests and secret-handler
  cancellation followed by late encrypted completion. This is measured execution,
  not exhaustive schedule coverage.
- C8: verified to stated coverage by assertions that public responses and RPC
  messages omit plaintext, including handler-error, encryption-error, and panic
  paths. Recovered panic values are not logged or returned. Connector-supplied
  ordinary error strings remain outside this guarantee.
- C9: verified by inspection. Existing handlers add constant adapter work and no
  encryption. Secret handlers perform O(P × R) encryption and retain O(P × R)
  ciphertext.

## Implementation-obligation addendum

- Registration stores an immutable set of secret return names derived from the
  schema. `Register` rejects schemas with secret return types; only
  `RegisterWithSecrets` can install their handlers.
- `InvokeActionWithWaitAndEncryption` clones and validates every recipient before
  creating an `OutstandingAction` or invoking the handler. The clone prevents
  caller mutation from changing recipients while detached work runs.
- The detached handler holds plaintext only in its local `actionHandlerResult`.
  `prepareActionResult` validates names and public-field exclusion, encrypts the
  values, and passes only ciphertext to `setOutcomeWithEncryptedData`.
- The `OutstandingAction` mutex owns status, public response, encrypted data,
  annotations, errors, and provisional-cancellation replacement. Invoke and status
  read all five result components under that mutex.
- `setOutcomeWithEncryptedData` clones the public response, ciphertext messages,
  and annotations before publication. Error, panic, and encryption-failure paths
  retain no encrypted or plaintext result.
- `ActionManager.encryptPlaintext` is initialized to
  `EncryptionManager.Encrypt`; the package test replaces it before invocation to
  prove an encryption failure settles as FAILED without publishing plaintext.
- Existing `InvokeAction`, `InvokeActionWithWait`, `GetActionStatus`, and
  `ActionHandler` signatures remain. Their adapters use the same registration,
  lifecycle, and settlement functions as secret handlers.
- The exported `connectorbuilder.ActionManager` method set remains unchanged.
  Connector dispatch asserts a private encrypted-result extension implemented by
  the SDK manager.
- Global and resource registries install the same `registeredActionHandler` and
  converge in `invokeRegisteredAction`.
- Local invocation has additive constructors/options that carry recipients into
  `ActionInvokeTask`; existing constructors delegate with no recipients.

## Commands

- `make protofmt && make protogen`: passed.
- `buf lint`: passed.
- `buf breaking --against '.git#branch=main'`: passed.
- `go test -count=1 ./pkg/actions ./pkg/connectorbuilder ./pkg/tasks/c1api ./pkg/tasks/local`:
  passed as focused package runs; the c1api assertion was run as
  `TestActionInvokeTaskThreadsEncryptionConfigs`.
- `go test -race -count=1 ./pkg/actions ./pkg/connectorbuilder`: passed.
- `golangci-lint run --timeout=3m ./pkg/actions/... ./pkg/connectorbuilder/...
  ./pkg/connectorrunner/... ./pkg/tasks/local/...`: passed with zero issues.
- `make lint`: the implementation's `nilerr` finding was fixed. The repository-wide
  run remains red on six pre-existing findings in `pkg/dotc1z`, `pkg/crypto`,
  `pkg/sync`, and the deprecated action-status name assignment.
- `go test -tags=baton_lambda_support ./...`: all affected packages passed. The
  repository-wide run remains red because
  `pkg/tasks/c1api.TestBootstrapSucceedsOnFirstAttempt` timed out and two
  `pkg/uhttp` tests could not write their SQLite database in this environment.

## Final audit

The post-CO-001 independent implementation read found no remaining HIGH or MEDIUM
defects. Its LOW gaps were reduced by adding the compile-time
`encryptedActionManager` assertion and a ciphertext ownership-clone test. Panic-log
redaction remains verified by direct code inspection plus an RPC payload test, not
by a captured-log oracle.
