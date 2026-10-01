# AGENTS.md — terminal-mcp

This repository provides a real persistent PTY terminal over MCP, including local/SSH command execution, human takeover, transcripts, web-terminal access, session routing, distributed forwarding, and embedding into another MCP server.

Because successful operation intentionally includes arbitrary shell execution, review this repository as **privileged infrastructure**. Do not flag “can execute commands” by itself as a vulnerability; instead determine whether execution, observation, routing, identity, takeover, or persisted output can escape the intended caller/session/host boundary.

## 1. Start with the actual product contract

Before consequential changes or review, read the relevant portions of:

1. `README.md`
2. `docs/EMBEDDING.md` when public API, mounting, authorization, or embedding behavior is involved
3. `config.example.toml` and `internal/config/config.go` for configuration semantics
4. the owning package and its tests

The Go module path is `github.com/fzxbl/terminal-mcp`; this repository may contain downstream/fork work, so do not casually rename the module or public package paths.

Keep documentation and implementation aligned when changing:
- public `mcpserver` APIs;
- tool names/descriptions/arguments;
- config keys/defaults;
- web-terminal routes;
- session-id/routing behavior;
- security or authorization expectations.

## 2. Core architecture and trust boundaries

The critical system flow is:

`MCP/HTTP request -> caller identity -> session ownership -> PTY/session state -> shell/SSH process -> append-only transcript -> bounded model output / output_ref -> human web terminal / takeover -> optional peer routing`

Review changes as a dynamic system. A locally correct function is not enough if it weakens another boundary.

Important assets and boundaries:

- host shell access in `mode=local`;
- remote shell access in `mode=ssh`;
- MCP endpoint and web terminal;
- session IDs and owner-node routing;
- per-client session ownership;
- human takeover ownership and write arbitration;
- append-only transcripts and audit/runtime logs;
- HMAC-signed `output_ref` snapshots;
- peer allowlist used for cross-node reverse proxy;
- caller identity headers supplied by a trusted gateway;
- config-provided shell/auth/resource-limit commands;
- child process groups and cleanup;
- embedding hosts that may provide their own authentication/authorization.

## 3. Security invariants

### Authentication and authorization

The service is unauthenticated by default. Reaching `/mcp` or the mounted terminal surface may be equivalent to shell access.

Do not imply terminal-mcp itself provides end-user authorization when it does not. In embedded deployments, authorization belongs to the host/gateway.

Treat these as privileged write/high-risk operations:
- `terminal_open`
- `terminal_send`
- `terminal_control`

Also treat takeover input and session close as consequential state-changing actions.

Flag changes that:
- expose privileged routes more broadly without an explicit contract;
- bypass host/gateway authorization assumptions;
- trust identity headers directly from untrusted clients;
- allow one caller to enumerate, observe, control, or close another caller's sessions.

### Client identity and session isolation

`internal/identity` derives session ownership from configured HTTP headers. Missing-header behavior and raw-vs-sha256 mode are part of the isolation contract.

Review for:
- missing-header fallback that broadens access;
- inconsistent identity calculation between MCP and web-terminal paths;
- header spoofing assumptions;
- empty/default identity collisions;
- ownership checks omitted from any session lookup or mutation path.

Identity headers are safe only when inserted/controlled by a trusted upstream gateway. Do not document client-supplied identity headers as trustworthy.

### Session IDs and distributed routing

Session IDs encode an owner-node address. Client-controlled session IDs therefore create an SSRF risk unless forwarding targets are constrained.

The current design intentionally permits reverse proxying only to known peers.

Flag:
- any path that dials the owner token without the peer allowlist;
- alternate forwarding code that bypasses `isAllowedProxyTarget`;
- redirect/proxy loops;
- forwarded-marker bypass;
- parsing differences between MCP routing and web-terminal routing;
- exposure of internal dial addresses where only a public URL should appear.

Unknown/unapproved owner tokens should fail locally as session-not-found, not trigger arbitrary outbound connections.

### Human takeover

Human and agent share the same PTY. Takeover must remain exclusive and race-safe.

Review changes around:
- acquire/release ownership;
- held-state visibility;
- agent writes while held;
- browser identity/owner matching;
- websocket/SSE reconnection;
- release by a different owner;
- takeover state after process/session death.

A convenience change must not allow simultaneous uncontrolled writes from agent and human.

### Output references

Oversized output is represented by an opaque HMAC-signed `output_ref` describing a fixed log range.

Preserve:
- cryptographic integrity;
- fixed-range semantics;
- fail-closed parsing;
- no unsigned/default-range fallback;
- process-lifetime key assumptions unless explicitly redesigned.

Do not turn `output_ref` into an arbitrary file/path/offset capability.

### Transcripts and logs

The append-only `.raw` transcript is the session-output source of truth; the in-memory buffer is only a bounded tail cache.

Review for:
- path traversal through session IDs or filenames;
- cross-session transcript reads;
- truncation or cache behavior that creates historical holes;
- log write failure being silently ignored when correctness depends on the transcript;
- retention cleanup deleting active/in-use state;
- secrets or credentials being copied into additional logs unnecessarily;
- unsafe file permissions or symlink behavior.

Do not replace the durable transcript with an in-memory-only representation.

## 4. Command construction and shell boundaries

Arbitrary user/agent commands are expected in terminal sessions. The security question is whether **server-controlled construction** introduces unintended shell interpretation or privilege.

### Local mode

A custom local command currently uses `sh -c`; that is deliberate arbitrary-command execution for the authorized caller.

Flag changes that allow untrusted data that was *not intended as the command* to be spliced into server-generated shell syntax.

### SSH mode

Review carefully:
- host/username argument construction;
- `ssh_opts`;
- `ssh_login_prologue`;
- quoting and positional-argument use;
- local-shell fallback behavior after prologue failure;
- remote command construction.

The prologue is trusted configuration, not untrusted request data. Host/session input must not gain an extra shell-parse layer unintentionally.

The documented default disables strict host-key checking. Treat that as a known product/security tradeoff, not an accidental discovery; do not silently change or further weaken it without updating configuration/docs/tests.

### Shell-switch detection and sentinel re-arm

`shell_switch_commands` and re-arm logic are best-effort shell-switch detection, not a full shell parser.

Review for:
- sentinel injection during password/passphrase/host-key prompts;
- false re-arm that corrupts an interactive program;
- missed re-arm that loses command-boundary/exit-code tracking;
- regressions across `ssh`, `su`, `sudo -i`, `docker exec`, `kubectl exec`, `nsenter`, `chroot`, and configured custom switches.

Do not “simplify” this mechanism without preserving interactive/auth prompt safety.

## 5. Resource and process safety

Resource limits are defense-in-depth, not a complete sandbox.

Current documented limits:
- `resource_limit_cmd` is commonly a model-invisible `ulimit`;
- it is injected at startup and re-arm/hard reset;
- privileged processes may raise hard limits;
- daemon-spawned process trees may escape inheritance;
- cgroup v2 is required for privilege-independent local process-tree limits.

Do not claim stronger containment than the implementation provides.

Process cleanup must reclaim the PTY and process group. Review for:
- orphan child/grandchild processes;
- zombie processes;
- local-vs-SSH cleanup differences;
- races between close, GC, output readers, and takeover;
- dead sessions remaining externally writable.

## 6. Output, backpressure, and denial-of-service boundaries

Preserve bounded model-facing output:
- `max_buffer_bytes` bounds the in-memory tail;
- full output remains on disk;
- `exec_output_max_bytes` returns an `output_ref` for oversized results;
- `terminal_explore` has hard server-side limits that callers cannot raise.

Flag:
- unbounded reads/allocations;
- reading entire runaway transcripts into memory;
- caller-controlled limits that bypass hard caps;
- regex/search behavior with pathological cost;
- fan-out or peer aggregation without time/size bounds;
- blocking operations that can wedge the MCP server.

Optimizations must not lose transcript fidelity or command-boundary correctness.

## 7. Distributed deployment

Sessions are node-pinned live resources; the system is not fully stateless.

Preserve:
- owner-node routing;
- peer allowlisting;
- public `terminal_url` separation from internal dial address;
- per-client isolation across peer forwarding;
- loop prevention;
- bounded/failure-tolerant `terminal_list` fan-out.

A node crash may lose live sessions on that node; do not claim migration/failover of the PTY unless such a mechanism is actually implemented and tested.

## 8. Public embedding API

Changes under `mcpserver/` may affect downstream hosts.

Treat these as compatibility-sensitive:
- `Init`
- `RegisterTools`
- `SetPublicBaseURL`
- `SetSelfAddr`
- `WithSessionRouting`
- `MountWebTerminal`
- `SetToolDescriptions`
- `StartIdleGC`
- `Shutdown`
- `NewHTTPHandler`

When changing them:
- preserve documented path/public-vs-internal-address semantics;
- update `docs/EMBEDDING.md` and README when behavior changes;
- add integration tests for shared-`/mcp` embedding and routing where relevant.

## 9. Change discipline

- Prefer narrow, reversible changes over broad rewrites.
- Do not weaken a security boundary merely to simplify code.
- Do not silently broaden defaults such as listen address, local-shell exposure, missing-identity behavior, peer forwarding, output limits, or retention.
- Configuration defaults are product behavior; changing them requires tests and docs.
- Keep source-of-truth ownership clear: transcript on disk, session state in the owning live process, config in config, public contract in `mcpserver`.
- Preserve failure truth. A failed write, invalid token, missing identity, unknown session owner, dead process, or failed re-arm must not be reported as success.

## 10. Testing requirements

For consequential changes, run at minimum:

```bash
go test ./...
```

Also run when relevant:

```bash
go test -race ./...
go vet ./...
```

Use `-race` especially for changes involving:
- session store/state;
- human takeover;
- PTY reads/writes;
- GC/close/shutdown;
- routing/fan-out;
- global config/runtime setters.

Add regression tests for the failure class being fixed. Test both success and denial/failure paths.

Security-sensitive changes should include adversarial tests where applicable:
- forged/cross-client session access;
- malicious session owner token / SSRF attempt;
- malformed/tampered `output_ref`;
- missing identity headers;
- takeover owner mismatch;
- oversized output/explore caps;
- transcript/path traversal;
- shell/SSH argument edge cases;
- route loops and unknown peers;
- process cleanup under interruption.

Do not treat a unit test alone as proof of secure deployment configuration.

## 11. Code review priorities

Prioritize findings in this order:

1. unauthorized shell/session access;
2. cross-client or cross-session leakage/control;
3. SSRF / unsafe peer routing;
4. command-construction or path injection;
5. takeover race / concurrent write corruption;
6. transcript/output_ref integrity or secret exposure;
7. process cleanup/resource exhaustion;
8. routing/embedding contract regressions;
9. command-boundary/sentinel/re-arm correctness;
10. compatibility and maintainability defects that can cause the above.

Do not bury material security/correctness findings under style nits.

For each material review finding, explain:
- the exact code path;
- the attacker/failure precondition;
- the resulting capability or incorrect state;
- the smallest robust repair;
- the missing regression test.

## 12. Known-intent vs vulnerability

Do not report these as vulnerabilities without an actual boundary bypass:

- an authorized caller can run arbitrary local commands;
- an authorized SSH session can run remote shell commands;
- transcripts contain terminal output;
- the service is unauthenticated when deliberately deployed without an authenticating gateway;
- configured `ssh_login_prologue` executes trusted operator configuration;
- resource limits are not a privilege-independent sandbox;
- live sessions are pinned to their owner node.

Instead, report cases where an **unauthorized or differently scoped actor** can gain those capabilities, where documented isolation can be bypassed, or where the implementation makes a stronger safety claim than it actually enforces.

## 13. Stop rule

Fail closed rather than guessing when a change cannot prove:
- caller/session ownership;
- forwarding target legitimacy;
- token integrity;
- path safety;
- takeover ownership;
- process/session lifecycle state;
- durable transcript behavior.

This repository's core promise is a powerful shared terminal that remains observable, bounded, attributable, and recoverable. Preserve those properties before optimizing convenience.
