# StaffX MCP API (v1, additive)

StaffX exposes a remote MCP surface at `/mcp` while keeping `/v1` unchanged.

## Feature flag

- Set `STAFFX_ENABLE_MCP=1` to enable MCP routes.
- When disabled, `/mcp` and MCP auth metadata routes are not registered.

## Endpoints

- `POST /mcp` JSON-RPC requests
- `GET /mcp` optional stream endpoint (`Accept: text/event-stream`)
- `DELETE /mcp` session termination
- `GET /.well-known/oauth-protected-resource`
- `GET /.well-known/oauth-protected-resource/mcp`

## Resources

- `staffx://me`
- `staffx://projects?page={n}&pageSize={n}&name={filter}`
- `staffx://projects/{projectId}`
- `staffx://threads?projectId={id}&page={n}&pageSize={n}`
- `staffx://threads/{threadId}?projectId={id}`
- `staffx://threads/{threadId}/matrix?projectId={id}`
- `staffx://assistant-runs/{runId}`
- `staffx://integrations`
- `staffx://integrations/{provider}/status`
- `staffx://threads/{threadId}/events?since={cursorOrTimestamp}&limit={n}`

## Tools

- `projects.create`
- `projects.check_name`
- `threads.create`
- `threads.update`
- `threads.delete`
- `threads.chat`
- `threads.matrix.patch_layout`
- `assistant_runs.start`
- `assistant_runs.cancel`
- `integrations.authorize_url`
- `integrations.disconnect`

## Behavior and parity

- MCP adapters call existing `/v1` routes through internal Fastify injection.
- Existing ACL and validation behavior remains source-of-truth in `/v1`.
- Internal worker lifecycle operations (`claim`, `complete`) are intentionally not exposed as MCP tools.

## Quick request examples

Initialize:

```bash
curl -s \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientInfo":{"name":"demo","version":"0.1.0"}}}' \
  http://localhost:3001/mcp
```

Tool list:

```bash
curl -s \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  http://localhost:3001/mcp
```

Resource read:

```bash
curl -s \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":3,"method":"resources/read","params":{"uri":"staffx://me"}}' \
  http://localhost:3001/mcp
```
