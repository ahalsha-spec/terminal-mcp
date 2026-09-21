package mcpserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/fzxbl/terminal-mcp/internal/audit"
	"github.com/fzxbl/terminal-mcp/internal/config"
	"github.com/fzxbl/terminal-mcp/internal/identity"
	"github.com/fzxbl/terminal-mcp/internal/job"
	"github.com/fzxbl/terminal-mcp/internal/session"
)

// 输入结构体：json tag 决定 MCP 工具参数名（snake_case），jsonschema tag 为参数描述。
// 官方 SDK 的泛型 AddTool 会据此自动推断 input schema 并在进入 handler 前完成校验。

type openInput struct {
	Mode    string `json:"mode" jsonschema:"session mode: \"local\" (spawn a shell/command on this host) or \"ssh\" (ssh into host and run bash)"`
	Command string `json:"command,omitempty" jsonschema:"local mode only: optional command to run instead of the default shell; ignored for ssh mode"`
	Host    string `json:"host,omitempty" jsonschema:"ssh mode only: target host to ssh into; required when mode=ssh, ignored for local mode"`
}

type sendInput struct {
	SessionID string `json:"session_id" jsonschema:"the session id returned by terminal_open"`
	Input     string `json:"input" jsonschema:"the command line to type into the session (a trailing newline is added automatically)"`
	WaitMs    int    `json:"wait_ms,omitempty" jsonschema:"max milliseconds to block waiting for the command to settle (default 30000, capped by server max_block_seconds)"`
}

type outputInput struct {
	SessionID string `json:"session_id" jsonschema:"the session id"`
	WaitMs    int    `json:"wait_ms,omitempty" jsonschema:"max milliseconds to wait before returning (default 0)"`
	Mode      string `json:"mode,omitempty" jsonschema:"\"tail\" (default: peek at the tail to judge if the command finished; does NOT advance the cursor) or \"since_last\" (deliver every new byte since the last since_last and advance the cursor)"`
}

type exploreInput struct {
	SessionID  string `json:"session_id" jsonschema:"the session id"`
	OutputRef  string `json:"output_ref" jsonschema:"the opaque reference returned in a truncated result"`
	Op         string `json:"op" jsonschema:"stat | read | grep"`
	LineOffset int    `json:"line_offset,omitempty" jsonschema:"read/grep start logical line (0-based; read accepts negative to count from the end)"`
	ByteOffset int    `json:"byte_offset,omitempty" jsonschema:"read: intra-line byte cursor for continuing a long-line read (use the byte_offset returned by the previous read)"`
	Limit      int    `json:"limit,omitempty" jsonschema:"max lines (read) or max matches (grep)"`
	Pattern    string `json:"pattern,omitempty" jsonschema:"grep: Go regular expression"`
	Before     int    `json:"before,omitempty" jsonschema:"grep: context lines before each match"`
	After      int    `json:"after,omitempty" jsonschema:"grep: context lines after each match"`
	MaxBytes   int    `json:"max_bytes,omitempty" jsonschema:"desired max body bytes (clamped to server hard cap)"`
}

type jobSubmitInput struct {
	Command        string   `json:"command" jsonschema:"non-interactive command to execute through the bounded job scheduler"`
	Cwd            string   `json:"cwd,omitempty" jsonschema:"optional working directory"`
	Class          string   `json:"class,omitempty" jsonschema:"normal | cpu_heavy | io_wait | background"`
	SupersedeKey   string   `json:"supersede_key,omitempty" jsonschema:"optional logical key; newer jobs from this caller cancel older queued/running jobs with the same key"`
	IdempotencyKey string   `json:"idempotency_key,omitempty" jsonschema:"optional retry key; identical replay returns the existing job, changed payload under the same key is rejected"`
	DependsOn      []string `json:"depends_on,omitempty" jsonschema:"existing job ids owned by this caller that must succeed before this job may run"`
	LockKeys       []string `json:"lock_keys,omitempty" jsonschema:"global mutual-exclusion resource keys; conflicting jobs wait rather than overlap"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty" jsonschema:"execution timeout; server clamps to its configured maximum"`
	WaitMs         int      `json:"wait_ms,omitempty" jsonschema:"bounded milliseconds to stay attached after admission; default 30000 and capped by max_block_seconds"`
	MaxBytes       int64    `json:"max_bytes,omitempty" jsonschema:"maximum output tail bytes returned by this submit call; clamped by the server"`
	Async          bool     `json:"async,omitempty" jsonschema:"set true only when immediate asynchronous admission is desired; otherwise job_submit waits boundedly and returns output in this same call"`
}

type jobBatchItemInput struct {
	Command        string   `json:"command" jsonschema:"non-interactive command to execute through the bounded job scheduler"`
	Cwd            string   `json:"cwd,omitempty" jsonschema:"optional working directory"`
	Class          string   `json:"class,omitempty" jsonschema:"normal | cpu_heavy | io_wait | background"`
	SupersedeKey   string   `json:"supersede_key,omitempty" jsonschema:"optional logical key; newer jobs from this caller cancel older queued/running jobs with the same key"`
	IdempotencyKey string   `json:"idempotency_key,omitempty" jsonschema:"optional retry key; identical replay returns the existing job, changed payload under the same key is rejected"`
	DependsOn      []string `json:"depends_on,omitempty" jsonschema:"existing job ids owned by this caller that must succeed before this job may run"`
	LockKeys       []string `json:"lock_keys,omitempty" jsonschema:"global mutual-exclusion resource keys; conflicting jobs wait rather than overlap"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty" jsonschema:"execution timeout; server clamps to its configured maximum"`
}

type jobBatchInput struct {
	Items []jobBatchItemInput `json:"items" jsonschema:"independent jobs to admit in one call; bounded by job_max_batch and normal queue limits"`
}

type jobSubmitOutput struct {
	job.Snapshot
	Output    string `json:"output,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}

type jobGraphNodeInput struct {
	NodeID         string   `json:"node_id" jsonschema:"caller-stable node id unique within this graph"`
	Command        string   `json:"command" jsonschema:"non-interactive command for this graph node"`
	Cwd            string   `json:"cwd,omitempty" jsonschema:"optional working directory"`
	Class          string   `json:"class,omitempty" jsonschema:"normal | cpu_heavy | io_wait | background"`
	SupersedeKey   string   `json:"supersede_key,omitempty" jsonschema:"optional logical key; cancels older same-purpose work before graph admission"`
	DependsOn      []string `json:"depends_on,omitempty" jsonschema:"node_id values in this same graph that must succeed first"`
	LockKeys       []string `json:"lock_keys,omitempty" jsonschema:"global mutual-exclusion resource keys"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty" jsonschema:"execution timeout; server clamps to configured maximum"`
}

type jobGraphInput struct {
	GraphKey string              `json:"graph_key,omitempty" jsonschema:"optional idempotency key for the entire graph; identical replay returns existing job handles"`
	Nodes    []jobGraphNodeInput `json:"nodes" jsonschema:"named DAG nodes; bounded by job_max_batch"`
}

type jobBatchItemOutput struct {
	Index int           `json:"index"`
	Job   *job.Snapshot `json:"job,omitempty"`
	Error string        `json:"error,omitempty"`
}

type jobBatchOutput struct {
	Results  []jobBatchItemOutput `json:"results"`
	Accepted int                  `json:"accepted"`
	Rejected int                  `json:"rejected"`
}

type jobIDInput struct {
	JobID string `json:"job_id" jsonschema:"the job id returned by job_submit"`
}

type jobResultInput struct {
	JobID    string `json:"job_id" jsonschema:"the job id returned by job_submit"`
	MaxBytes int64  `json:"max_bytes,omitempty" jsonschema:"maximum tail bytes to return; clamped by the server"`
	WaitMs   int    `json:"wait_ms,omitempty" jsonschema:"max milliseconds to wait for the job to reach a terminal state before returning; default 30000, capped by server max_block_seconds. Use job_status for an immediate non-blocking snapshot."`
}

type controlInput struct {
	SessionID string `json:"session_id" jsonschema:"the session id"`
	Key       string `json:"key" jsonschema:"control key or recovery action. Control keys (written to the PTY as the corresponding control byte): ctrl-c (SIGINT, interrupt the running command), ctrl-d (EOF, end input / exit a REPL or shell), ctrl-z (SIGTSTP, suspend to background), ctrl-\\ (SIGQUIT, quit with core), ctrl-l (clear screen), ctrl-u (erase to line start), ctrl-k (erase to line end), ctrl-a (move to line start), ctrl-e (move to line end), ctrl-w (erase previous word), ctrl-r (reverse history search), ctrl-g (bell / cancel current edit or search), tab (completion), esc (Escape), enter (Enter), backspace. Recovery actions: flush (drop queued input + clear the current line + Enter), hard (reopen the shell), rearm (re-inject the sentinel prompt after you switched into a new shell, e.g. after su/docker exec/chroot, if the session appears stuck)."`
}

type sessionIDInput struct {
	SessionID string `json:"session_id" jsonschema:"the session id"`
}

type emptyInput struct{}

// listOutput 包装会话列表：官方 SDK 要求工具输出为 JSON object，顶层数组不被接受。
type capacityOutput struct {
	Used      int `json:"used" jsonschema:"local admission slots currently occupied after dead-session reconciliation"`
	Max       int `json:"max" jsonschema:"configured local admission limit"`
	Available int `json:"available" jsonschema:"local admission slots currently available"`
}

type jobListOutput struct {
	Jobs     []job.Snapshot `json:"jobs" jsonschema:"jobs owned by this caller"`
	Capacity job.Capacity   `json:"capacity" jsonschema:"global bounded job-plane capacity"`
}

type listOutput struct {
	Sessions []map[string]string `json:"sessions" jsonschema:"active sessions with status snapshot (session_id, host, status, idle_seconds, held)"`
	Capacity capacityOutput      `json:"capacity" jsonschema:"local instance admission capacity after dead-session reconciliation"`
}

// 工具描述（英文）。涵盖 PTY 会话、local/ssh 模式、
// 人工接管 held 语义、output 的 tail vs since_last 差异等关键指引。
const (
	descOpen = "Start a persistent real PTY session and return {session_id, state, terminal_url}. " +
		"mode=local spawns a shell (or the given command) on this host; mode=ssh opens 'ssh <host>' running bash (host is required). " +
		"The server absorbs normal shell readiness before returning; state is normally idle. If an unusually slow startup still returns loading, wait meaningfully before one terminal_status check rather than rapid polling. " +
		"terminal_url is a read-only web terminal a human can open to watch the session live, and optionally 'take over' to type commands manually. " +
		"While a human has taken over, the session returns held=true and the model's send/close/control are blocked; use terminal_output(mode=since_last) to observe what the human is doing."

	descSend = "Type a command into the session and block up to wait_ms (capped by max_block_seconds) for it to settle. " +
		"Returns this command's new output plus state (running/idle/dead), prompt and exit_code. " +
		"state=running means the command has not finished yet (e.g. a large core still loading) - keep polling with terminal_output. " +
		"If the output is too large the return is truncated: truncated=true and an output_ref is returned and this output already advanced the delivery cursor; use terminal_explore (op=stat/grep/read) to inspect it selectively; you can continue running commands afterwards. " +
		"If held=true the session is under human takeover: this call was NOT executed, do not retry write operations; wait or do other work and watch with terminal_output(mode=since_last) until held clears."

	descOutput = "Observe session output without relying on injected markers. Two modes, do not mix them to 'fetch everything':\n" +
		"mode=tail (default, 'a quick glance'): returns only the last ~tail_bytes of the current screen to judge whether the command finished (check state and prompt). It does NOT advance the since_last cursor and can be called repeatedly.\n" +
		"mode=since_last ('fetch the complete increment'): returns every new byte since the previous since_last call, losing nothing, and advances the delivery cursor. If a single increment is too large the return is truncated (truncated=true + output_ref) and the cursor has already advanced past it; inspect it with terminal_explore.\n" +
		"Correct way to fetch a full result: poll with tail until state=idle, then read with since_last.\n" +
		"To see what a human did during takeover you MUST use mode=since_last: it reconstructs every command the human typed as \"[rc=n] $ command\" (with exit code) together with its output. held=true means a human is currently in control."

	descExplore = "Read-only exploration of an oversized result referenced by output_ref (returned by terminal_send / terminal_output when a single output exceeds the size cap). " +
		"It inspects a fixed snapshot and does NOT advance the since_last cursor; the next since_last will NOT re-return this result. " +
		"Do NOT read the whole result sequentially into context: first op=stat (size_bytes/line_count/max_line_bytes), then op=grep (pattern + before/after context) to locate, then op=read a local slice (line_offset 0-based, limit lines; a negative line_offset reads from the end; byte_offset continues a long-line read using the byte_offset from the previous read). " +
		"Errors are usually at the end - use op=read with a negative line_offset. pattern is a Go regular expression; max_bytes is clamped to the server cap."

	descControl = "Send a control key to the session, or perform a recovery action. " +
		"Control keys are written to the PTY as raw control bytes and work even while a command is running: " +
		"ctrl-c (SIGINT, interrupt), ctrl-d (EOF), ctrl-z (suspend), ctrl-\\ (SIGQUIT), ctrl-l (clear screen), " +
		"ctrl-u / ctrl-k (erase to line start/end), ctrl-a / ctrl-e (move to line start/end), ctrl-w (erase word), " +
		"ctrl-r (reverse search), ctrl-g (cancel), tab, esc, enter, backspace. " +
		"Recovery actions: flush (drop queued input + clear the current line + Enter), hard (reopen the shell) and " +
		"rearm (re-inject the sentinel prompt after you switched into a new shell, e.g. after su/docker exec/chroot, if the session appears stuck). " +
		"If held=true the session is under human takeover: this call was NOT executed; wait until held clears."

	descStatus = "Lightweight status query (empty output). Returns state, prompt, exit_code and held. " +
		"held=true means a human has taken over: the model should pause write operations and only read until held becomes false; " +
		"use terminal_output(mode=since_last) to see what the human executed."

	descJobSubmit = "Submit bounded non-interactive machine work without consuming a persistent PTY, then by default stay attached for up to 30000 ms and return terminal state plus bounded output in this same MCP call. " +
		"Prefer this single-call path for normal tests, builds, searches, file transforms and deterministic scripts; set async=true only when immediate asynchronous admission is genuinely required. wait_ms is capped by max_block_seconds and max_bytes bounds returned output. " +
		"The scheduler still bounds physical execution, applies per-caller/global queue limits, and round-robins across callers. Use class=cpu_heavy for sustained CPU work, io_wait for mostly waiting/I/O work, background for deferrable work, otherwise normal. " +
		"supersede_key cancels stale same-purpose work; idempotency_key deduplicates exact retries and rejects changed payloads; depends_on gates execution on prior successful jobs; lock_keys serialize conflicting resources."
	descJobBatchSubmit = "Submit a bounded batch of non-interactive jobs in one MCP call. " +
		"Use this instead of many serial tool calls when work is separable. Each item supports idempotency, existing-job dependencies and lock keys. " +
		"Admission remains subject to the same global/per-caller queue limits and adaptive physical scheduler; each item reports accepted or rejected explicitly."
	descJobGraphSubmit = "Submit a bounded named dependency DAG in one MCP call. " +
		"Independent nodes may run concurrently; depends_on edges serialize only true prerequisites; lock_keys prevent conflicting resource overlap. " +
		"The graph is validated and queue-admitted atomically, so cycles or capacity failures leave no partial graph. graph_key makes exact retries idempotent and rejects changed definitions."
	descJobStatus = "Read one job's state immediately without waiting. Use this when you explicitly need a non-blocking snapshot."
	descJobResult = "Wait boundedly for a job to reach a terminal state, then read a bounded tail of its disk-backed output. " +
		"wait_ms defaults to 30000 and is capped by server max_block_seconds; if the bound expires first, the current queued/running snapshot is returned so the caller can do other work instead of rapid-polling. " +
		"Large output stays local; max_bytes is clamped by the server. Prefer this over repeated job_status/job_result polling when waiting for normal job completion."
	descJobCancel = "Cancel queued or running work owned by this caller. Running process groups are terminated; use when work becomes stale or is superseded."
	descJobList   = "List this caller's jobs plus global scheduler capacity: active/max_active, queued/max_queued, and cpu_heavy_active/max_cpu_heavy."

	descClose = "Close the session, releasing its child process and concurrency slot. " +
		"If held=true the session is under human takeover: this call was NOT executed; wait until held clears."

	descList = "List all sessions on this instance with their status snapshot (session_id, host, status, idle_seconds, held) plus local capacity {used,max,available}. " +
		"held=true means that session is under human takeover; pause write operations until it clears."
)

// defaultDescriptions 各工具的内置默认描述，供 resolveDesc 在无覆盖时回退。
var defaultDescriptions = map[string]string{
	"job_submit":       descJobSubmit,
	"job_batch_submit": descJobBatchSubmit,
	"job_graph_submit": descJobGraphSubmit,
	"job_status":       descJobStatus,
	"job_result":       descJobResult,
	"job_cancel":       descJobCancel,
	"job_list":         descJobList,
	"terminal_open":    descOpen,
	"terminal_send":    descSend,
	"terminal_output":  descOutput,
	"terminal_explore": descExplore,
	"terminal_control": descControl,
	"terminal_status":  descStatus,
	"terminal_close":   descClose,
	"terminal_list":    descList,
}

var (
	descMu           sync.RWMutex
	descProgOverride = map[string]string{}
)

// SetToolDescriptions 以编程方式覆盖工具描述（key 为工具名，如 terminal_open）。供把本模块嵌入外部
// MCP 的宿主在 RegisterTools/NewHTTPHandler 之前按自家话术改写工具说明。空串条目忽略；传 nil 清空。
// 优先级：编程覆盖 > 配置文件 tool_descriptions > 内置默认。并发安全。
func SetToolDescriptions(over map[string]string) {
	descMu.Lock()
	defer descMu.Unlock()
	descProgOverride = map[string]string{}
	for k, v := range over {
		if v != "" {
			descProgOverride[k] = v
		}
	}
}

// resolveDesc 计算工具对外描述：编程覆盖 > 配置文件 tool_descriptions > 内置默认。
func resolveDesc(name string) string {
	descMu.RLock()
	v, ok := descProgOverride[name]
	descMu.RUnlock()
	if ok {
		return v
	}
	if c := config.Get().ToolDescriptions; c != nil {
		if v, ok := c[name]; ok && v != "" {
			return v
		}
	}
	return defaultDescriptions[name]
}

func jobResultWait(waitMs int) time.Duration {
	maxMs := config.Get().MaxBlockSeconds * 1000
	if maxMs <= 0 {
		maxMs = 30000
	}
	if waitMs <= 0 {
		waitMs = 30000
	}
	if waitMs > maxMs {
		waitMs = maxMs
	}
	return time.Duration(waitMs) * time.Millisecond
}

func submitAndMaybeWait(owner string, in jobSubmitInput) (jobSubmitOutput, error) {
	snap, err := job.Submit(owner, job.SubmitArgs{
		Command: in.Command, Cwd: in.Cwd, Class: in.Class, SupersedeKey: in.SupersedeKey,
		IdempotencyKey: in.IdempotencyKey, DependsOn: in.DependsOn, LockKeys: in.LockKeys,
		Timeout: time.Duration(in.TimeoutSeconds) * time.Second,
	})
	out := jobSubmitOutput{Snapshot: snap}
	if err != nil || in.Async {
		return out, err
	}
	res, err := job.ResultForWait(owner, snap.JobID, in.MaxBytes, jobResultWait(in.WaitMs))
	if err != nil {
		return out, err
	}
	out.Snapshot = res.Snapshot
	out.Output = res.Output
	out.Truncated = res.Truncated
	return out, nil
}

// registerTools 在给定 server 上注册全部 terminal_* 工具。
// 每个 handler 计算结果后调用 audit.Logger 记录一条审计。CallerIP 从 HTTP 请求头解析
// （官方 SDK 通过 CallToolRequest.Extra.Header 把 HTTP header 透传给每个工具 handler）。
func registerTools(server *mcp.Server, a *audit.Logger) {
	mcp.AddTool(server, &mcp.Tool{Name: "job_submit", Description: resolveDesc("job_submit")},
		func(_ context.Context, req *mcp.CallToolRequest, in jobSubmitInput) (*mcp.CallToolResult, jobSubmitOutput, error) {
			owner, ok := ownerSig(req)
			if !ok {
				return nil, jobSubmitOutput{}, fmt.Errorf("missing required identity header(s): %v", config.Get().Identity.Headers)
			}
			out, err := submitAndMaybeWait(owner, in)
			e := baseEntry(req, "job_submit", map[string]any{
				"class": in.Class, "cwd": in.Cwd, "supersede_key": in.SupersedeKey,
				"idempotency_key": in.IdempotencyKey, "depends_on": len(in.DependsOn), "lock_keys": len(in.LockKeys),
				"timeout_seconds": in.TimeoutSeconds, "async": in.Async, "wait_ms": in.WaitMs, "max_bytes": in.MaxBytes,
			})
			if err != nil {
				e.Error = err.Error()
			} else {
				e.State = out.State
				e.Bytes = len(out.Output)
			}
			a.Log(e)
			return nil, out, err
		})

	mcp.AddTool(server, &mcp.Tool{Name: "job_batch_submit", Description: resolveDesc("job_batch_submit")},
		func(_ context.Context, req *mcp.CallToolRequest, in jobBatchInput) (*mcp.CallToolResult, jobBatchOutput, error) {
			owner, ok := ownerSig(req)
			if !ok {
				return nil, jobBatchOutput{}, fmt.Errorf("missing required identity header(s): %v", config.Get().Identity.Headers)
			}
			if len(in.Items) == 0 {
				return nil, jobBatchOutput{}, fmt.Errorf("items cannot be empty")
			}
			if len(in.Items) > config.Get().JobMaxBatch {
				return nil, jobBatchOutput{}, fmt.Errorf("batch too large: %d > %d", len(in.Items), config.Get().JobMaxBatch)
			}
			out := jobBatchOutput{Results: make([]jobBatchItemOutput, 0, len(in.Items))}
			for i, item := range in.Items {
				snap, err := job.Submit(owner, job.SubmitArgs{
					Command: item.Command, Cwd: item.Cwd, Class: item.Class, SupersedeKey: item.SupersedeKey,
					IdempotencyKey: item.IdempotencyKey, DependsOn: item.DependsOn, LockKeys: item.LockKeys,
					Timeout: time.Duration(item.TimeoutSeconds) * time.Second,
				})
				r := jobBatchItemOutput{Index: i}
				if err != nil {
					r.Error = err.Error()
					out.Rejected++
				} else {
					snapCopy := snap
					r.Job = &snapCopy
					out.Accepted++
				}
				out.Results = append(out.Results, r)
			}
			e := baseEntry(req, "job_batch_submit", map[string]any{"items": len(in.Items)})
			e.State = fmt.Sprintf("accepted=%d rejected=%d", out.Accepted, out.Rejected)
			a.Log(e)
			return nil, out, nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "job_graph_submit", Description: resolveDesc("job_graph_submit")},
		func(_ context.Context, req *mcp.CallToolRequest, in jobGraphInput) (*mcp.CallToolResult, job.GraphResult, error) {
			owner, ok := ownerSig(req)
			if !ok {
				return nil, job.GraphResult{}, fmt.Errorf("missing required identity header(s): %v", config.Get().Identity.Headers)
			}
			if len(in.Nodes) == 0 {
				return nil, job.GraphResult{}, fmt.Errorf("nodes cannot be empty")
			}
			if len(in.Nodes) > config.Get().JobMaxBatch {
				return nil, job.GraphResult{}, fmt.Errorf("graph too large: %d > %d", len(in.Nodes), config.Get().JobMaxBatch)
			}
			args := job.GraphSubmitArgs{GraphKey: in.GraphKey, Nodes: make([]job.GraphNodeArgs, 0, len(in.Nodes))}
			for _, node := range in.Nodes {
				args.Nodes = append(args.Nodes, job.GraphNodeArgs{
					NodeID: node.NodeID, Command: node.Command, Cwd: node.Cwd, Class: node.Class,
					SupersedeKey: node.SupersedeKey, DependsOn: node.DependsOn, LockKeys: node.LockKeys,
					Timeout: time.Duration(node.TimeoutSeconds) * time.Second,
				})
			}
			res, err := job.SubmitGraph(owner, args)
			e := baseEntry(req, "job_graph_submit", map[string]any{"graph_key": in.GraphKey, "nodes": len(in.Nodes)})
			if err != nil {
				e.Error = err.Error()
			} else {
				e.State = "accepted"
			}
			a.Log(e)
			return nil, res, err
		})

	mcp.AddTool(server, &mcp.Tool{Name: "job_status", Description: resolveDesc("job_status")},
		func(_ context.Context, req *mcp.CallToolRequest, in jobIDInput) (*mcp.CallToolResult, job.Snapshot, error) {
			owner, ok := ownerSig(req)
			if !ok {
				return nil, job.Snapshot{}, fmt.Errorf("missing required identity header(s): %v", config.Get().Identity.Headers)
			}
			snap, err := job.Status(owner, in.JobID)
			e := baseEntry(req, "job_status", map[string]any{"job_id": in.JobID})
			if err != nil {
				e.Error = err.Error()
			} else {
				e.State = snap.State
			}
			a.Log(e)
			return nil, snap, err
		})

	mcp.AddTool(server, &mcp.Tool{Name: "job_result", Description: resolveDesc("job_result")},
		func(_ context.Context, req *mcp.CallToolRequest, in jobResultInput) (*mcp.CallToolResult, job.Result, error) {
			owner, ok := ownerSig(req)
			if !ok {
				return nil, job.Result{}, fmt.Errorf("missing required identity header(s): %v", config.Get().Identity.Headers)
			}
			wait := jobResultWait(in.WaitMs)
			res, err := job.ResultForWait(owner, in.JobID, in.MaxBytes, wait)
			e := baseEntry(req, "job_result", map[string]any{
				"job_id": in.JobID, "max_bytes": in.MaxBytes, "wait_ms": wait.Milliseconds(),
			})
			if err != nil {
				e.Error = err.Error()
			} else {
				e.State = res.Snapshot.State
				e.Bytes = len(res.Output)
			}
			a.Log(e)
			return nil, res, err
		})

	mcp.AddTool(server, &mcp.Tool{Name: "job_cancel", Description: resolveDesc("job_cancel")},
		func(_ context.Context, req *mcp.CallToolRequest, in jobIDInput) (*mcp.CallToolResult, job.Snapshot, error) {
			owner, ok := ownerSig(req)
			if !ok {
				return nil, job.Snapshot{}, fmt.Errorf("missing required identity header(s): %v", config.Get().Identity.Headers)
			}
			snap, err := job.Cancel(owner, in.JobID)
			e := baseEntry(req, "job_cancel", map[string]any{"job_id": in.JobID})
			if err != nil {
				e.Error = err.Error()
			} else {
				e.State = snap.State
			}
			a.Log(e)
			return nil, snap, err
		})

	mcp.AddTool(server, &mcp.Tool{Name: "job_list", Description: resolveDesc("job_list")},
		func(_ context.Context, req *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, jobListOutput, error) {
			owner, ok := ownerSig(req)
			if !ok {
				return nil, jobListOutput{}, fmt.Errorf("missing required identity header(s): %v", config.Get().Identity.Headers)
			}
			jobs, capacity := job.List(owner)
			e := baseEntry(req, "job_list", nil)
			e.State = "ok"
			a.Log(e)
			return nil, jobListOutput{Jobs: jobs, Capacity: capacity}, nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "terminal_open", Description: resolveDesc("terminal_open")},
		func(_ context.Context, req *mcp.CallToolRequest, in openInput) (*mcp.CallToolResult, map[string]string, error) {
			owner, ok := ownerSig(req)
			if !ok {
				return nil, nil, fmt.Errorf("missing required identity header(s): %v", config.Get().Identity.Headers)
			}
			res, err := session.Open(in.Mode, in.Command, in.Host, owner)
			e := baseEntry(req, "terminal_open", map[string]any{
				"mode": in.Mode, "host": in.Host, "command": in.Command,
			})
			if err != nil {
				e.Error = err.Error()
			} else {
				e.State = res["state"]
			}
			a.Log(e)
			return nil, res, err
		})

	mcp.AddTool(server, &mcp.Tool{Name: "terminal_send", Description: resolveDesc("terminal_send")},
		func(_ context.Context, req *mcp.CallToolRequest, in sendInput) (*mcp.CallToolResult, session.Envelope, error) {
			owner, ok := ownerSig(req)
			if !ok || !authorizeOwner(owner, in.SessionID) {
				env := session.Envelope{State: "dead", Error: "session not found"}
				logEnv(a, req, "terminal_send", map[string]any{"session_id": in.SessionID}, env)
				return nil, env, nil
			}
			env := session.Send(in.SessionID, in.Input, in.WaitMs)
			logEnv(a, req, "terminal_send", map[string]any{
				"session_id": in.SessionID, "input": in.Input, "wait_ms": in.WaitMs,
			}, env)
			return nil, env, nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "terminal_output", Description: resolveDesc("terminal_output")},
		func(_ context.Context, req *mcp.CallToolRequest, in outputInput) (*mcp.CallToolResult, session.Envelope, error) {
			owner, ok := ownerSig(req)
			if !ok || !authorizeOwner(owner, in.SessionID) {
				env := session.Envelope{State: "dead", Error: "session not found"}
				logEnv(a, req, "terminal_output", map[string]any{"session_id": in.SessionID, "wait_ms": in.WaitMs, "mode": in.Mode}, env)
				return nil, env, nil
			}
			env := session.Read(in.SessionID, session.ReadArgs{Mode: in.Mode, WaitMs: in.WaitMs})
			logEnv(a, req, "terminal_output", map[string]any{
				"session_id": in.SessionID, "wait_ms": in.WaitMs, "mode": in.Mode,
			}, env)
			return nil, env, nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "terminal_explore", Description: resolveDesc("terminal_explore")},
		func(_ context.Context, req *mcp.CallToolRequest, in exploreInput) (*mcp.CallToolResult, session.Envelope, error) {
			owner, ok := ownerSig(req)
			if !ok || !authorizeOwner(owner, in.SessionID) {
				env := session.Envelope{State: "dead", Error: "session not found"}
				logEnv(a, req, "terminal_explore", map[string]any{"session_id": in.SessionID, "op": in.Op, "output_ref": in.OutputRef}, env)
				return nil, env, nil
			}
			env := session.Explore(in.SessionID, session.ExploreArgs{
				OutputRef: in.OutputRef, Op: in.Op,
				LineOffset: in.LineOffset, ByteOffset: in.ByteOffset, Limit: in.Limit, Pattern: in.Pattern,
				Before: in.Before, After: in.After, MaxBytes: in.MaxBytes,
			})
			logEnv(a, req, "terminal_explore", map[string]any{
				"session_id": in.SessionID, "op": in.Op, "output_ref": in.OutputRef,
			}, env)
			return nil, env, nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "terminal_control", Description: resolveDesc("terminal_control")},
		func(_ context.Context, req *mcp.CallToolRequest, in controlInput) (*mcp.CallToolResult, session.Envelope, error) {
			owner, ok := ownerSig(req)
			if !ok || !authorizeOwner(owner, in.SessionID) {
				env := session.Envelope{State: "dead", Error: "session not found"}
				logEnv(a, req, "terminal_control", map[string]any{"session_id": in.SessionID, "key": in.Key}, env)
				return nil, env, nil
			}
			env := session.Control(in.SessionID, in.Key)
			logEnv(a, req, "terminal_control", map[string]any{
				"session_id": in.SessionID, "key": in.Key,
			}, env)
			return nil, env, nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "terminal_status", Description: resolveDesc("terminal_status")},
		func(_ context.Context, req *mcp.CallToolRequest, in sessionIDInput) (*mcp.CallToolResult, session.Envelope, error) {
			owner, ok := ownerSig(req)
			if !ok || !authorizeOwner(owner, in.SessionID) {
				env := session.Envelope{State: "dead", Error: "session not found"}
				logEnv(a, req, "terminal_status", map[string]any{"session_id": in.SessionID}, env)
				return nil, env, nil
			}
			env := session.Status(in.SessionID)
			logEnv(a, req, "terminal_status", map[string]any{"session_id": in.SessionID}, env)
			return nil, env, nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "terminal_close", Description: resolveDesc("terminal_close")},
		func(_ context.Context, req *mcp.CallToolRequest, in sessionIDInput) (*mcp.CallToolResult, session.Envelope, error) {
			owner, ok := ownerSig(req)
			if !ok || !authorizeOwner(owner, in.SessionID) {
				env := session.Envelope{State: "dead", Error: "session not found"}
				logEnv(a, req, "terminal_close", map[string]any{"session_id": in.SessionID}, env)
				return nil, env, nil
			}
			env := session.Close(in.SessionID)
			logEnv(a, req, "terminal_close", map[string]any{"session_id": in.SessionID}, env)
			return nil, env, nil
		})

	mcp.AddTool(server, &mcp.Tool{Name: "terminal_list", Description: resolveDesc("terminal_list")},
		func(_ context.Context, req *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, listOutput, error) {
			owner, ok := ownerSig(req)
			if !ok {
				return nil, listOutput{}, fmt.Errorf("missing required identity header(s): %v", config.Get().Identity.Headers)
			}
			local := session.List(owner)
			var all []map[string]string
			if req.Extra != nil && req.Extra.Header.Get(forwardedHeader) == "1" {
				all = local
			} else {
				all = fanoutList(local, peerList(), reqHeader(req), owner)
			}
			e := baseEntry(req, "terminal_list", nil)
			e.Bytes = len(all)
			a.Log(e)
			used, max, available := session.Capacity()
			return nil, listOutput{Sessions: all, Capacity: capacityOutput{Used: used, Max: max, Available: available}}, nil
		})
}

var (
	signerMu  sync.Mutex
	theSigner *identity.Signer
)

// signer 惰性构造身份签名器（读一次配置）。加锁避免 stateless 下并发请求同时初始化的数据竞争。
func signer() *identity.Signer {
	signerMu.Lock()
	defer signerMu.Unlock()
	if theSigner == nil {
		c := config.Get()
		theSigner = identity.New(c.Identity.Headers, c.Identity.Mode, c.Identity.OnMissing)
	}
	return theSigner
}

// ownerSig 从请求头算出调用方归属签名；ok=false 表示按 reject 策略缺头、应拒绝。
func ownerSig(req *mcp.CallToolRequest) (string, bool) {
	return signer().Signature(reqHeader(req))
}

// authorizeOwner 校验签名 owner 是否为会话 id 的属主。false → 越权或本机无此会话，按 not found 处理。
func authorizeOwner(owner, id string) bool {
	got, found := session.Owner(id)
	if !found {
		return false
	}
	return got == owner
}

// baseEntry 构造带 CallerIP 与调用方标识（X-MCP-USER）的审计条目骨架。
func baseEntry(req *mcp.CallToolRequest, tool string, params map[string]any) audit.Entry {
	h := reqHeader(req)
	return audit.Entry{
		CallerIP: callerIP(h),
		User:     mcpUser(h),
		Tool:     tool,
		Params:   params,
	}
}

// mcpUser 从 X-MCP-USER 头解析每次调用的调用方标识（无则空）。
func mcpUser(h http.Header) string {
	if h == nil {
		return ""
	}
	return h.Get("X-MCP-USER")
}

// logEnv 记录返回 Envelope 的工具审计（State/ExitCode/Held/Bytes/Error）。
func logEnv(a *audit.Logger, req *mcp.CallToolRequest, tool string, params map[string]any, env session.Envelope) {
	e := baseEntry(req, tool, params)
	e.State = env.State
	e.Held = env.Held
	e.Bytes = len(env.Output)
	e.Error = env.Error
	e.ExitCode = env.ExitCode
	a.Log(e)
}

// reqHeader 取本次调用透传的 HTTP header（无则 nil）。
func reqHeader(req *mcp.CallToolRequest) http.Header {
	if req == nil || req.Extra == nil {
		return nil
	}
	return req.Extra.Header
}

// callerIP 从 HTTP header 解析调用方 IP：优先 X-Forwarded-For（取首个），
// 再 X-Real-Ip，最后由 audit 中间件注入的 X-Pty-Bridge-Mcp-Remoteaddr（原始连接地址）。
func callerIP(h http.Header) string {
	if h == nil {
		return ""
	}
	if xff := h.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	if xr := h.Get("X-Real-Ip"); xr != "" {
		return strings.TrimSpace(xr)
	}
	return h.Get("X-Pty-Bridge-Mcp-Remoteaddr")
}

// remoteAddrMiddleware 在无转发头时，把 TCP 连接的对端 IP 注入 X-Pty-Bridge-Mcp-Remoteaddr，
// 使每个工具 handler 都能经 CallToolRequest.Extra.Header 拿到调用方 IP 做审计。
func remoteAddrMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Forwarded-For") == "" && r.Header.Get("X-Real-Ip") == "" {
			ip, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				ip = r.RemoteAddr
			}
			r.Header.Set("X-Pty-Bridge-Mcp-Remoteaddr", ip)
		}
		next.ServeHTTP(w, r)
	})
}
