# Threat Model — terminal-mcp

**Status:** repository security-review authority  
**Scope:** `terminal-mcp` standalone and embedded deployments  
**Audience:** maintainers, code reviewers, Codex Security Review, operators embedding or exposing terminal-mcp

This document describes the security model terminal-mcp is intended to enforce and the boundaries it intentionally does **not** enforce. It should be read together with `AGENTS.md`, `README.md`, `docs/EMBEDDING.md`, and `config.example.toml`.

terminal-mcp deliberately gives an authorized caller a real shell. Therefore the primary security question is not “can this code execute commands?” It is:

> Can an unauthorized or differently scoped actor execute, observe, redirect, interfere with, or persist access to a terminal session outside the authority they were intended to have?

---

## 1. System summary

terminal-mcp exposes persistent PTY sessions through MCP.

High-level flow:

```text
MCP / HTTP caller
      |
      v
trusted auth/gateway boundary (deployment responsibility)
      |
      v
caller identity headers
      |
      v
terminal-mcp tool authorization
      |
      v
session ownership + live PTY
      |
      +--> local shell
      |
      +--> ssh client -> remote shell
      |
      +--> append-only transcript
      |
      +--> bounded MCP output / HMAC output_ref
      |
      +--> human web terminal
      |       |
      |       +--> SSE observation
      |       +--> takeover state
      |       +--> WebSocket PTY input
      |
      +--> optional peer routing to owner node
```

Live PTYs are process-local and node-pinned. Session metadata/routing may span nodes, but a running PTY cannot migrate between nodes.

---

## 2. Security objectives

The system should preserve all of the following:

1. **Authorized execution** — only an authorized caller may open or control terminal sessions.
2. **Session isolation** — one caller must not observe, control, close, or enumerate another caller's sessions.
3. **Single-writer takeover** — human takeover and agent writes must not race into uncontrolled simultaneous PTY input.
4. **Routing confinement** — client-controlled session IDs must not become arbitrary network destinations.
5. **Output integrity** — `output_ref` tokens must not be forgeable or broadened into arbitrary ranges/files.
6. **Transcript integrity** — persistent terminal output must remain correctly associated with its session and must not expose arbitrary filesystem paths.
7. **Bounded resource use** — model-facing output, in-memory buffering, blocking, and exploration must remain bounded.
8. **Truthful state** — dead, held, failed, missing, truncated, or unroutable sessions must not be reported as successful/idle.
9. **Deterministic cleanup** — closing/GC/shutdown should reclaim the intended PTY/process group as far as the operating system/session model permits.
10. **Observable privileged actions** — tool calls and session activity should remain auditable without creating unnecessary additional secret exposure.

---

## 3. Assets

Security-sensitive assets include:

- local host shell authority;
- SSH credentials and resulting remote-shell authority;
- live PTY input/output;
- session IDs;
- per-client ownership identity;
- human takeover ownership/state;
- terminal transcripts;
- audit/runtime logs;
- `output_ref` signing key and signed ranges;
- trusted peer addresses;
- trusted gateway identity headers;
- config values such as SSH options, login prologue, resource limits, paths, and retention;
- public terminal URLs and internal node dial addresses;
- child processes and process groups spawned by sessions.

---

## 4. Actors

### 4.1 Authorized MCP caller

An agent/user that has passed the deployment's authentication/authorization layer and is allowed to use terminal tools.

This actor is intentionally capable of arbitrary shell execution within the authority of the terminal-mcp process or configured SSH account.

### 4.2 Authorized human operator

A person allowed to open the web terminal, observe a session, and optionally take over its PTY.

### 4.3 Another authenticated tenant/caller

A legitimate caller who must remain isolated from sessions owned by other callers.

### 4.4 Unauthenticated network attacker

A party able to reach an exposed HTTP endpoint but not entitled to shell access.

If terminal-mcp is deployed without an authenticating boundary, this actor may become equivalent to an authorized shell user. That is a deployment failure, not a protection terminal-mcp currently supplies by itself.

### 4.5 Malicious or compromised authorized agent

An authorized caller attempting resource exhaustion, persistence, unexpected network access, shell escapes, or misuse of intentionally granted shell authority.

The system should retain boundedness and attribution where practical, but terminal-mcp is **not a sandbox against an authorized shell principal**.

### 4.6 Malicious web origin

A website attempting to drive a user's browser into injecting PTY input or altering takeover state.

### 4.7 Compromised peer node

A configured distributed peer that is no longer trustworthy.

Peers are currently inside the cluster trust boundary.

### 4.8 Local privileged attacker / root

A party with host/root-level authority.

Host root is outside terminal-mcp's protection boundary and can bypass process/resource/file controls.

---

## 5. Deployment trust assumptions

### 5.1 Authentication is external

terminal-mcp is unauthenticated by default.

A production or shared deployment MUST either:

- bind only to a trusted local interface/network; or
- place the complete terminal-mcp surface behind a trusted authenticating and authorizing gateway.

**The gateway must protect both the MCP endpoint and the human terminal surface.**

Protecting only `/mcp` while leaving `/view/terminal/...` reachable is insufficient.

### 5.2 Identity headers are trusted only from the gateway

Configured identity headers such as `X-MCP-USER` are meaningful only when a trusted upstream:

- authenticates the caller;
- injects or overwrites those headers;
- prevents direct clients from choosing arbitrary identity values.

The header value itself is not proof of identity.

### 5.3 Browser `owner` values are not authentication

The takeover `owner` value is a concurrency/ownership token used to prevent multiple human operators from simultaneously controlling the same held session.

It must **not** be treated as authentication or authorization.

The enclosing web-terminal route must already be protected by the deployment trust boundary.

### 5.4 Peer nodes are trusted infrastructure

Configured peers are allowed routing destinations.

The current distributed design assumes:

- peer addresses are operator-controlled;
- peer nodes are trusted;
- the inter-node network is trusted or independently protected.

If peers communicate over an untrusted network, operators should add transport authentication/encryption (for example mTLS or a trusted service mesh). Plain HTTP peer routing is not a confidentiality/integrity boundary.

---

## 6. Trust boundaries and data flows

### TB-1 — External caller -> HTTP/MCP service

Threats:
- unauthenticated shell access;
- forged identity headers;
- oversized/malformed requests;
- abuse of high-risk tools.

Required controls:
- trusted deployment auth boundary;
- fail-closed identity handling;
- bounded request/tool behavior;
- tool-level ownership checks.

### TB-2 — MCP tool -> session owner

Threats:
- cross-tenant session enumeration/control;
- session ID guessing/reuse;
- inconsistent authorization between tools.

Required controls:
- every session-specific tool checks caller identity against session owner;
- unauthorized access returns a non-enumerating result such as session-not-found;
- `terminal_list` filters by owner.

### TB-3 — Human browser -> web terminal

Threats:
- unauthorized transcript observation;
- unauthorized takeover;
- cross-origin WebSocket PTY injection;
- concurrent human writers;
- takeover release by non-owner.

Existing controls include:
- single-holder takeover state;
- owner matching for held sessions;
- same-origin WebSocket checks;
- disabling agent write/control while held.

Required deployment control:
- authenticate/authorize the entire web-terminal route.

### TB-4 — Session -> local shell

Threats:
- arbitrary host commands;
- process/resource exhaustion;
- child-process persistence;
- filesystem/network access at process privilege.

Arbitrary commands are intended for authorized callers.

Required controls:
- narrow deployment privilege;
- optional `disable_local_mode` where host shell access is unnecessary;
- optional resource limits/cgroups;
- deterministic session/process cleanup;
- external OS/container hardening where stronger isolation is required.

### TB-5 — Session -> SSH target

Threats:
- argument/shell injection in server-generated command construction;
- credential leakage;
- host spoofing / MITM;
- remote persistence/resource use.

Existing design properties:
- request-supplied host is passed as an argument rather than interpolated into the prologue shell program;
- trusted `ssh_login_prologue` is configuration;
- default SSH options currently disable strict host-key checking.

The disabled host-key checking default is a known security tradeoff. It allows first-use convenience but weakens resistance to SSH MITM. Do not represent the default as host-authenticated SSH.

### TB-6 — Session -> transcript/log storage

Threats:
- path traversal;
- cross-session reads;
- symlink/file-permission problems;
- disk exhaustion;
- secret retention;
- deletion of active history.

Required controls:
- generated/validated paths;
- session ownership before transcript access through user-facing surfaces;
- append-only transcript semantics;
- bounded retention and operator disk monitoring;
- avoid unnecessary duplication of sensitive command/output data.

### TB-7 — Oversized output -> `output_ref`

Threats:
- token forgery;
- range expansion;
- arbitrary path/file read;
- stale/default fallback after parse failure;
- cross-session misuse.

Existing controls:
- process-random HMAC key;
- signed fixed scope;
- fail-closed parsing;
- session authorization still required by `terminal_explore`.

Security note:
the signed scope is a range capability, not an authentication token. Session ownership remains mandatory.

### TB-8 — Client-controlled session ID -> peer routing

Threat:
- SSRF to arbitrary `host:port`.

Existing control:
- owner tokens are proxied only when they match known configured/discovered peers.

Required invariant:
no alternate MCP/web-terminal forwarding path may dial a client-derived owner address without the peer allowlist.

### TB-9 — Peer -> peer

Threats:
- malicious peer impersonation;
- identity-header tampering;
- route loops;
- caller confusion;
- session enumeration.

Current assumption:
configured peers are trusted.

The forwarded marker is a routing-loop/control signal, not authentication. External clients must not gain privilege merely by setting it.

---

## 7. Principal threat scenarios

| ID | Threat | Impact | Existing / required mitigation |
| --- | --- | --- | --- |
| T1 | Service exposed without authentication | Arbitrary shell access | Loopback/trusted network or authenticating gateway; document high-risk tools |
| T2 | Client spoofs identity header | Cross-tenant session control | Trusted gateway must overwrite/inject identity; missing identity defaults to reject |
| T3 | Session-specific tool omits owner check | Cross-session read/write/close | Central ownership checks on every session operation; negative tests |
| T4 | Crafted session ID routes to arbitrary address | SSRF / internal network access | Peer allowlist before proxy; unknown owner handled locally |
| T5 | Web terminal exposed while MCP is protected | Transcript disclosure / PTY takeover | Protect entire mounted terminal route with same gateway |
| T6 | Malicious external website opens takeover WS | Drive-by command injection | Same-origin WebSocket validation plus authenticated terminal route |
| T7 | Second human operator writes during takeover | Command corruption | Exclusive hold owner; owner-match on WS/release |
| T8 | Agent writes while human has control | Mixed commands / corruption | Held state blocks model send/control/close |
| T9 | Forged `output_ref` expands readable range | Unauthorized output disclosure | HMAC signature; fail-closed parsing; fixed range; session ownership |
| T10 | Transcript path manipulation | Arbitrary file read/write | Session-ID/path validation; never accept arbitrary transcript path from caller |
| T11 | Huge terminal output exhausts memory | Availability failure | Bounded memory tail + output limits + selective explore |
| T12 | Huge transcript exhausts disk | Host availability failure | Retention, monitoring, filesystem quotas; memory cap alone is insufficient |
| T13 | Malicious grep/read request causes excessive CPU/memory | Availability failure | Hard server-side explore limits; bounded regex/results |
| T14 | Shell/SSH construction adds unintended parse layer | Command injection | Preserve argument boundaries; treat config shell snippets as trusted config only |
| T15 | Sentinel/re-arm injected during auth prompt | Credential/session corruption | Auth-prompt detection; conservative re-arm; regression tests |
| T16 | Hostile output spoofs command boundary | False idle/exit-code state | Keep sentinel protocol robust/unpredictable enough; test adversarial output |
| T17 | Close/GC leaves child process tree alive | Persistence/resource leak | Process-group termination, PTY close, wait/reap; integration/race tests |
| T18 | Remote/daemonized process survives local session | Residual remote/local activity | Document limitation; do not claim session close is a universal remote process kill |
| T19 | `ulimit` described as sandbox | False security assumption | Document privilege/daemon escape; use cgroup/container/OS isolation when required |
| T20 | Peer traffic traverses untrusted network in plaintext | Session/control confidentiality or integrity loss | Trusted network or mTLS/service mesh |
| T21 | Audit/transcript logs capture secrets | Credential/data disclosure | Minimize duplicate logging, protect file permissions/retention, avoid logging secrets unnecessarily |
| T22 | Node failure treated as transparent failover | Lost live session / misleading state | Explicitly model sessions as node-pinned; report failure truthfully |

---

## 8. Web-terminal specific model

The web terminal is security-sensitive because it exposes both observation and optional PTY input.

### Observation

SSE can disclose the entire terminal transcript/scrollback.

Therefore the read-only page is **not harmless public metadata**. It must inherit the same authorization boundary as terminal execution.

### Takeover

Takeover changes who may write to the PTY.

Security requirements:

- only a live session may be taken over;
- only one human owner may hold it at a time;
- the current owner alone may release an owned hold;
- WebSocket input must be rejected unless the session is currently held;
- owned holds require matching owner on the write channel;
- agent write/control/close operations must remain blocked while held;
- leaving takeover restores normal model-side PTY behavior.

### Browser-origin protection

The WebSocket upgrade accepts same-origin browser connections (or clients without an `Origin` header).

This mitigates browser drive-by injection but does not authenticate a user. Authentication remains the deployment gateway's responsibility.

Review any future CORS/origin relaxation as a security-sensitive change.

---

## 9. Session identity and confidentiality

Session IDs identify routing/session state but should not be treated as bearer authentication.

Knowing a session ID must not be sufficient to:

- read output;
- explore an output reference;
- send commands;
- send control keys;
- close a session;
- list another user's session;
- take over a terminal in a deployment that otherwise promises authenticated web access.

MCP tools must authorize by caller ownership independently of session-ID knowledge.

For the web terminal, deployment authentication must protect the route because the current in-process web-terminal trust model does not independently reproduce MCP caller authorization.

---

## 10. Shell and SSH construction rules

The system intentionally accepts arbitrary command text for an authorized local session.

That does not justify accidental shell interpretation of unrelated values.

Review requirements:

- request `command` is allowed to be shell syntax in local mode;
- SSH `host` must remain an argv value, not interpolated into a shell program;
- trusted config such as `ssh_login_prologue` may contain shell syntax by design;
- untrusted request values must not be concatenated into the prologue program;
- `ssh_opts` are operator configuration and should not be request-controlled;
- remote bootstrap command text should remain controlled/constant unless a reviewed API change deliberately exposes it.

---

## 11. Transcript, audit, and secret handling

Terminal sessions may contain:

- passwords typed during interactive auth;
- tokens;
- private keys or key material printed by commands;
- environment variables;
- source code;
- production logs;
- customer/user data.

The transcript exists specifically to provide observability and replay, so sensitive output retention is partly inherent.

Security requirements:

- document retention;
- use restrictive deployment filesystem permissions;
- avoid copying full PTY output into additional audit logs unless required;
- audit metadata should be sufficient to attribute actions without needlessly duplicating secret-bearing output;
- cleanup must not follow attacker-controlled symlinks or paths;
- operators should treat transcript and log directories as sensitive data stores.

---

## 12. Resource exhaustion model

terminal-mcp includes bounded model-facing output and memory protections, but arbitrary shell authority can still consume machine resources.

Existing controls include:

- `max_sessions`;
- idle GC;
- `max_buffer_bytes`;
- `exec_output_max_bytes`;
- hard `terminal_explore` limits;
- `max_block_seconds`;
- optional `resource_limit_cmd`;
- transcript/log retention.

Residual risks:

- an authorized process can generate disk output faster than retention removes it;
- `ulimit` cannot constrain privileged processes that raise limits;
- daemon-spawned processes may not inherit session limits;
- network, filesystem, GPU, device, and kernel resources are not comprehensively sandboxed.

For stronger isolation use OS/container controls, quotas, cgroup v2, separate service accounts, and network policy.

---

## 13. Distributed deployment model

The distributed design scales request handling but does not migrate live PTYs.

Security invariants:

- only configured/discovered peers may be proxied;
- public base URL and internal self/dial address remain distinct concepts;
- caller identity must survive forwarding correctly;
- peer fan-out must not leak other callers' sessions;
- routing must remain single-hop/loop-safe;
- peer failures must degrade truthfully.

Security assumption:
all accepted peers belong to the same trust domain unless a stronger authenticated inter-peer protocol is added.

---

## 14. Accepted limitations / non-goals

The following are currently accepted boundaries, not bugs by themselves:

1. An authorized terminal caller can execute arbitrary commands.
2. Local mode exposes the privileges of the terminal-mcp process.
3. SSH mode exposes the privileges of the configured remote account.
4. The standalone HTTP service is unauthenticated unless deployment adds auth.
5. The web terminal shares that deployment trust model.
6. Session transcripts may contain sensitive terminal data.
7. Default SSH options disable strict host-key checking.
8. `resource_limit_cmd` / `ulimit` is not a privilege-independent sandbox.
9. Host root can bypass terminal-mcp isolation.
10. Daemonized/detached processes can outlive the direct PTY process tree in some cases.
11. Live PTYs cannot migrate to another node after owner-node failure.
12. Configured peers are currently trusted.
13. A malicious authorized shell principal is not fully contained by terminal-mcp.

Security Review should report a finding when code **expands one of these limitations beyond the documented boundary**, silently weakens compensating controls, or claims stronger protection than actually exists.

---

## 15. Security-sensitive files / areas

Changes in these areas deserve elevated review:

- `mcpserver/tools.go` — tool authorization, audit, identity propagation
- `mcpserver/routing.go` — peer routing / SSRF boundary
- `mcpserver/fanout.go` — cross-node aggregation / identity propagation
- `mcpserver/mount.go` — route exposure
- `internal/identity/` — caller ownership
- `internal/session/` — session state, takeover, command lifecycle, GC
- `internal/terminal/` — web observation/takeover/WebSocket input
- `internal/outputref/` — signed output capabilities
- `internal/pty/` and `internal/oplog/` — process/transcript integrity
- `internal/config/` — security defaults
- `internal/logging/` and audit code — sensitive operational records
- `docs/EMBEDDING.md` — downstream authorization contract
- `config.example.toml` — deployed security posture examples/defaults

---

## 16. Required adversarial regression tests

Security-relevant changes should add or preserve tests for applicable cases:

- missing required identity header;
- spoofed/different owner attempting session access;
- cross-owner `terminal_list` isolation;
- crafted session owner token targeting non-peer address;
- peer-loop prevention;
- forwarded request retaining correct owner identity;
- malformed/tampered `output_ref`;
- output_ref cannot bypass session authorization;
- takeover by owner A followed by owner B;
- non-owner takeover release;
- agent send/control/close while held;
- WebSocket from foreign browser origin;
- WebSocket with wrong takeover owner;
- session death during takeover;
- transcript/session path traversal attempts;
- oversized output and hard explore caps;
- pathological explore/grep inputs;
- shell/SSH argument edge cases;
- auth-prompt-safe sentinel re-arm;
- hostile output that resembles sentinel/prompt markers;
- close/GC/shutdown process cleanup;
- race tests for session store, takeover, close, routing, and global runtime setters.

Use `go test -race ./...` for concurrency-sensitive changes.

---

## 17. Review decision rules

A security finding should identify:

1. the boundary being crossed;
2. the actor/precondition;
3. the concrete exploit or failure path;
4. the resulting unauthorized capability, leakage, integrity loss, or availability impact;
5. the smallest robust fix;
6. the regression test that would prove the fix.

Do not report generic “command injection” merely because an authorized caller's command reaches `sh -c`. Report it when data that is *not intended to be executable command text* gains shell interpretation or crosses an authorization boundary.

Do not report “no authentication” as a novel code defect when reviewing the documented standalone trust model. Report it if a code/doc/config change falsely implies authentication exists, exposes a previously protected route, or defeats an embedding host's auth boundary.

---

## 18. Release/security checklist

Before accepting a consequential security-sensitive change, confirm:

- [ ] MCP caller ownership remains enforced on every session-specific tool.
- [ ] Web-terminal exposure assumptions are unchanged or explicitly documented.
- [ ] Peer routing cannot dial arbitrary client-selected addresses.
- [ ] Takeover remains single-owner and blocks agent writes.
- [ ] `output_ref` remains integrity-protected and fail-closed.
- [ ] Transcript paths/readback remain session-scoped.
- [ ] Output/explore hard caps remain server-controlled.
- [ ] Process cleanup and dead-session behavior remain truthful.
- [ ] Security defaults were not silently broadened.
- [ ] Public/embedding docs match behavior.
- [ ] Relevant negative/adversarial tests exist.
- [ ] `go test ./...` passes.
- [ ] `go test -race ./...` is run for concurrency-sensitive changes.

---

## 19. Threat-model maintenance

Update this document when any of these materially change:

- authentication/authorization ownership;
- identity-header model;
- web-terminal access model;
- takeover semantics;
- session-ID format or routing;
- peer trust/transport;
- output_ref construction;
- transcript/log persistence;
- local/SSH launch behavior;
- process/resource containment;
- distributed session model;
- embedding/public API security contract.

A review that changes one of those boundaries without updating this threat model is incomplete.
