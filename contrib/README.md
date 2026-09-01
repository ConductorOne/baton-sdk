# contrib

Staging area for connector source that is destined for its own repository but has nowhere to live
yet.

Nothing here is part of the `github.com/conductorone/baton-sdk` Go module. Each subdirectory is a
self-contained module with its own `go.mod`, so the SDK's `go build ./...`, `go test ./...` and
`golangci-lint run` do not descend into it, and the SDK's own dependency graph is unaffected.

## `baton-appstoreconnect`

A complete Apple App Store Connect connector (CXH-2377). It belongs in
`ConductorOne/baton-appstoreconnect`, created from
[baton-starter-pack](https://github.com/ConductorOne/baton-starter-pack) like every other connector.

To extract it into that repository once it exists:

```bash
git subtree split --prefix=contrib/baton-appstoreconnect -b appstoreconnect-export
git push git@github.com:ConductorOne/baton-appstoreconnect.git appstoreconnect-export:main
```

Then, in the new repository, run `make update-deps` to vendor dependencies (connector repos vendor;
this staged copy does not, to keep the diff reviewable) and let the `generate-baton-metadata`
workflow produce `baton_capabilities.json` and `config_schema.json`.

Once the connector lives in its own repository, delete this directory.
