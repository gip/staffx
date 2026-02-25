# StaffX MCP Auth (Auth0 delegated user OAuth)

StaffX MCP uses bearer JWT access tokens issued by Auth0.

## Token expectations

- Issuer: `https://${AUTH0_DOMAIN}/`
- Audience:
  - default: `AUTH0_AUDIENCE`
  - optional override/addition: `AUTH0_MCP_AUDIENCE`
- Scope claim: standard OAuth `scope` string

MCP requests accept either audience when `AUTH0_MCP_AUDIENCE` is configured.

## Scope model

- `staffx:projects:read`
- `staffx:projects:write`
- `staffx:threads:read`
- `staffx:threads:write`
- `staffx:matrix:read`
- `staffx:matrix:write`
- `staffx:runs:read`
- `staffx:runs:write`
- `staffx:integrations:read`
- `staffx:integrations:write`
- `staffx:events:read`

Each MCP tool/resource is mapped to required scopes server-side before adapter execution.

## Challenge behavior

Missing/invalid token:

- HTTP `401`
- `WWW-Authenticate: Bearer ... error="invalid_token" ...`

Insufficient scope:

- HTTP `403`
- `WWW-Authenticate: Bearer ... error="insufficient_scope" ... scope="..."`

Both responses include `resource_metadata` pointing to:

- `/.well-known/oauth-protected-resource/mcp`

## Protected Resource Metadata endpoints

- `GET /.well-known/oauth-protected-resource`
- `GET /.well-known/oauth-protected-resource/mcp`

These advertise:

- `resource`
- `authorization_servers`
- `bearer_methods_supported`
- `scopes_supported`
- `audiences_supported`

## Auth0 setup checklist

1. Define all MCP scopes on the Auth0 API.
2. Ensure PKCE Authorization Code flow is enabled for client apps.
3. Set API audience and, if needed, a dedicated MCP audience.
4. Confirm token includes `scope` and any org claims used by tenant isolation policy.

## Security notes

- Scope checks do not replace ACL checks.
- `/v1` authorization remains authoritative via existing project/thread access rules.
- MCP event reads are thread-scoped in v1 MCP surface.
