# `baton-appstoreconnect`

`baton-appstoreconnect` is a connector for [Apple App Store Connect](https://appstoreconnect.apple.com)
built using the [Baton SDK](https://github.com/conductorone/baton-sdk). It syncs the people who can
reach your app distribution pipeline — team members, the roles they hold, and which apps each of
them can see — and can provision that access back.

App Store Connect users are personal Apple IDs. They sit outside SSO and SCIM entirely, so without a
connector there is no central view of who holds Admin or App Manager on the account that controls
code signing, releases and financial reports.

## Prerequisites

You need an App Store Connect API key with the **Admin** role. Team-scoped keys are created under
[Users and Access → Integrations → App Store Connect API](https://appstoreconnect.apple.com/access/integrations/api).
Creating one gives you three things:

| Value | Where to find it |
| --- | --- |
| **Issuer ID** | Shown once at the top of the Integrations page. |
| **Key ID** | The row for the key you created. |
| **Private key** (`.p8`) | Downloadable exactly once, when the key is created. |

The `.p8` file is a long-lived bearer-equivalent secret: anyone holding it can act as an Admin on
your App Store Connect account. Store it in a secret manager, never in a repository, and rotate it
on the same schedule as your other privileged credentials. Revoking a key in App Store Connect takes
effect immediately.

Only a key with the Admin role can read or modify users. A key with any lesser role authenticates
successfully but gets `403 FORBIDDEN_ERROR` on `/v1/users`; the connector reports that as a
permission problem rather than a bad credential.

## Getting started

```bash
baton-appstoreconnect \
  --key-id 2X9R4HXF34 \
  --issuer-id 57246542-96fe-1a63-e053-0824d011072a \
  --private-key-path ./AuthKey_2X9R4HXF34.p8
```

Configuration can also be supplied through the environment: `BATON_KEY_ID`, `BATON_ISSUER_ID`, and
either `BATON_PRIVATE_KEY` (the PEM contents) or `BATON_PRIVATE_KEY_PATH` (a path on disk).

## What gets synced

| Resource | Source | Notes |
| --- | --- | --- |
| **User** | `GET /v1/users` | One paginated pass; roles arrive inline, so there is no per-user fan-out. |
| **User** (pending) | `GET /v1/userInvitations` | Outstanding invitations become users with `PENDING` status. |
| **Role** | fixed enum | Apple has no roles endpoint; the enum is static, so no discovery is needed. |
| **App** | `GET /v1/apps` | Used for per-app (`visibleApps`) access. |

Entitlements:

* `role:<ROLE>:assigned` — the user holds that App Store Connect role.
* `app:<id>:visible` — the user can see that app. A user with `allAppsVisible` holds this
  entitlement on **every** app, because they really can see every app; leaving them out would
  understate access at review time.

## Provisioning

| Operation | API | Notes |
| --- | --- | --- |
| Grant/revoke a role | `PATCH /v1/users/{id}` | Full-replace on the `roles` array (see below). |
| Grant/revoke app access | `PATCH /v1/users/{id}` | Full-replace on the `visibleApps` relationship. |
| Create an account | `POST /v1/userInvitations` | Sends an invitation; see below. |
| Delete an account | `DELETE /v1/users/{id}` | Falls back to `DELETE /v1/userInvitations/{id}` for a pending invitee. |

## Limitations and behaviour worth knowing

**Account creation is an invitation, and it is asynchronous.** Apple has no way to create a user
directly. `CreateAccount` sends an invitation, and the invitee only becomes a user once they accept
it with their own Apple ID. The resource the connector returns is the *invitation*, marked
`PENDING`. When it is accepted, the invitation disappears and a user record with a **different id**
takes its place, correlated by email address. First and last name are required by Apple, so they are
required fields on the account creation schema.

**Identity correlation is email-only.** Users are personal Apple IDs. There is no stable external
identifier to map onto an IdP identity, so correlation rests on the email address.

**Role and app updates are read-modify-write.** Apple replaces the whole `roles` array and the whole
`visibleApps` relationship on every `PATCH`, so the connector reads the user's current state before
writing the new one. That window is not atomic: a change made in the App Store Connect UI between
the read and the write is overwritten. ConductorOne serializes provisioning per user, so the
realistic exposure is a concurrent human edit, not concurrent grants.

**`ACCOUNT_HOLDER` cannot be managed through the API.** It is synced so it shows up in reviews, but
granting or revoking it fails with a clear error instead of a confusing Apple rejection.

**Revoking one app from an all-apps user is refused.** The user holds the app entitlement because
`allAppsVisible` is set, not because of a per-app assignment. Removing a single app would mean
turning `allAppsVisible` off and rebuilding the list of everything they should keep — a far larger
change than the request asked for, so the connector refuses and says what to do instead.

**Apple caps the inlined `visibleApps` relationship at 50 apps per user.** The connector detects the
truncation from the relationship's `meta.paging.total` and falls back to
`GET /v1/users/{id}/visibleApps` for that user. Trusting a truncated list would silently drop grants
and, during provisioning, silently revoke apps.

**No SCIM and no user webhooks.** Access changes are only visible via polling sync.

**Rate limits.** Apple publishes roughly 3,600 requests/hour per key through the `x-rate-limit`
header (`user-hour-lim` / `user-hour-rem`), plus an undocumented short-window throttle. Every
response's budget is reported to ConductorOne as a rate-limit annotation, and 429s carry
`Retry-After` when Apple sends one.

**Tokens expire every 20 minutes.** App Store Connect rejects a JWT that claims a longer life, so
the connector mints ES256 tokens with a 15-minute lifetime and rolls onto a new one two minutes
before expiry. A sync longer than a token's life is fine.

## Development

```bash
make build     # regenerates pkg/config/conf.gen.go, then builds
make test      # go test ./...
make lint      # golangci-lint run
```

There are no live-tenant tests in `go test`: the unit tests drive a stand-in App Store Connect API
over `httptest`, including the JWT signing path, pagination, rate-limit parsing, error mapping and
every provisioning payload. Live grant/revoke checks run in CI once an Admin API key is stored in
repository secrets.

## Contributing, support and issues

We started Baton because we were tired of taking screenshots and manually building spreadsheets. We
welcome contributions, and ideas, no matter how small — our goal is to make identity and permissions
sprawl less painful for everyone. If you have questions, problems, or ideas: please open a GitHub
Issue!

See [CONTRIBUTING.md](https://github.com/ConductorOne/baton/blob/main/CONTRIBUTING.md) for more details.
