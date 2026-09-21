package mcpserver

import (
	"fmt"
	"time"

	"github.com/fzxbl/terminal-mcp/internal/config"
	"github.com/fzxbl/terminal-mcp/internal/job"
)

type jobGraphNodeOutput struct {
	NodeID    string       `json:"node_id"`
	Job       job.Snapshot `json:"job"`
	Output    string       `json:"output,omitempty"`
	Truncated bool         `json:"truncated,omitempty"`
	Error     string       `json:"error,omitempty"`
}

type jobGraphOutput struct {
	GraphID                 string               `json:"graph_id"`
	GraphKey                string               `json:"graph_key,omitempty"`
	Nodes                   []jobGraphNodeOutput `json:"nodes"`
	TerminalCount           int                  `json:"terminal_count"`
	PendingCount            int                  `json:"pending_count"`
	FailedCount             int                  `json:"failed_count"`
	ReadbackErrorCount      int                  `json:"readback_error_count,omitempty"`
	UnobservedTerminalCount int                  `json:"unobserved_terminal_count"`
	AllTerminal             bool                 `json:"all_terminal"`
	FollowupRequired        bool                 `json:"followup_required"`
	PendingJobIDs           []string             `json:"pending_job_ids,omitempty"`
}

type jobDrainInput struct {
	JobIDs         []string `json:"job_ids,omitempty" jsonschema:"optional caller-owned job ids to reread deterministically; omit to discover and drain this caller's outstanding unobserved work"`
	WaitMs         int      `json:"wait_ms,omitempty" jsonschema:"shared bounded wait horizon; default 30000 ms and capped by max_block_seconds"`
	MaxBytesPerJob int64    `json:"max_bytes_per_job,omitempty" jsonschema:"maximum output tail bytes per returned job; additionally bounded by the aggregate response budget"`
}

type jobDrainItemOutput struct {
	Job       job.Snapshot `json:"job"`
	Output    string       `json:"output,omitempty"`
	Truncated bool         `json:"truncated,omitempty"`
	Error     string       `json:"error,omitempty"`
}

type jobDrainOutput struct {
	Selected                         int                  `json:"selected"`
	Results                          []jobDrainItemOutput `json:"results"`
	TerminalCount                    int                  `json:"terminal_count"`
	PendingCount                     int                  `json:"pending_count"`
	FailedCount                      int                  `json:"failed_count"`
	ReadbackErrorCount               int                  `json:"readback_error_count,omitempty"`
	AllTerminal                      bool                 `json:"all_terminal"`
	FollowupRequired                 bool                 `json:"followup_required"`
	PendingJobIDs                    []string             `json:"pending_job_ids,omitempty"`
	RemainingUnobservedTerminalCount int                  `json:"remaining_unobserved_terminal_count"`
}

func terminalJobState(state string) bool {
	switch state {
	case "succeeded", "failed", "canceled", "timed_out", "blocked":
		return true
	default:
		return false
	}
}

func failedTerminalState(state string) bool {
	return terminalJobState(state) && state != "succeeded"
}

func aggregateMaxBytesPerJob(requested int64, jobs int) int64 {
	limit := config.Get().JobResultMaxBytes
	if limit <= 0 {
		limit = 64 << 10
	}
	if requested <= 0 || requested > limit {
		requested = limit
	}
	if jobs < 1 {
		jobs = 1
	}
	share := limit / int64(jobs)
	if share < 1 {
		share = 1
	}
	if requested > share {
		requested = share
	}
	return requested
}

func waitResultToDeadline(owner, jobID string, maxBytes int64, deadline time.Time) (job.Result, error) {
	remaining := time.Until(deadline)
	if remaining < 0 {
		remaining = 0
	}
	return job.ResultForWait(owner, jobID, maxBytes, remaining)
}

func classifyBatchOutput(out *jobBatchOutput) {
	out.TerminalCount = 0
	out.PendingCount = 0
	out.FailedCount = 0
	out.ReadbackErrorCount = 0
	out.UnobservedTerminalCount = 0
	out.PendingJobIDs = nil
	for i := range out.Results {
		r := &out.Results[i]
		if r.Error != "" {
			out.ReadbackErrorCount++
		}
		if r.Job == nil {
			continue
		}
		if terminalJobState(r.Job.State) {
			out.TerminalCount++
			if failedTerminalState(r.Job.State) {
				out.FailedCount++
			}
			if !r.Job.Observed {
				out.UnobservedTerminalCount++
			}
		} else {
			out.PendingCount++
			out.PendingJobIDs = append(out.PendingJobIDs, r.Job.JobID)
		}
	}
	out.AllTerminal = out.Accepted > 0 && out.PendingCount == 0 && out.ReadbackErrorCount == 0
	out.FollowupRequired = out.PendingCount > 0 || out.ReadbackErrorCount > 0 || out.UnobservedTerminalCount > 0
}

func submitBatchAndMaybeWait(owner string, in jobBatchInput) (jobBatchOutput, error) {
	if len(in.Items) == 0 {
		return jobBatchOutput{}, fmt.Errorf("items cannot be empty")
	}
	if len(in.Items) > config.Get().JobMaxBatch {
		return jobBatchOutput{}, fmt.Errorf("batch too large: %d > %d", len(in.Items), config.Get().JobMaxBatch)
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
	if in.Async {
		classifyBatchOutput(&out)
		return out, nil
	}

	deadline := time.Now().Add(jobResultWait(in.WaitMs))
	maxBytes := aggregateMaxBytesPerJob(in.MaxBytesPerJob, out.Accepted)
	for i := range out.Results {
		r := &out.Results[i]
		if r.Job == nil {
			continue
		}
		res, err := waitResultToDeadline(owner, r.Job.JobID, maxBytes, deadline)
		if err != nil {
			r.Error = err.Error()
			continue
		}
		snapCopy := res.Snapshot
		r.Job = &snapCopy
		r.Output = res.Output
		r.Truncated = res.Truncated
	}
	classifyBatchOutput(&out)
	return out, nil
}

func classifyGraphOutput(out *jobGraphOutput) {
	out.TerminalCount = 0
	out.PendingCount = 0
	out.FailedCount = 0
	out.ReadbackErrorCount = 0
	out.UnobservedTerminalCount = 0
	out.PendingJobIDs = nil
	for i := range out.Nodes {
		r := &out.Nodes[i]
		if r.Error != "" {
			out.ReadbackErrorCount++
		}
		if terminalJobState(r.Job.State) {
			out.TerminalCount++
			if failedTerminalState(r.Job.State) {
				out.FailedCount++
			}
			if !r.Job.Observed {
				out.UnobservedTerminalCount++
			}
		} else {
			out.PendingCount++
			out.PendingJobIDs = append(out.PendingJobIDs, r.Job.JobID)
		}
	}
	out.AllTerminal = len(out.Nodes) > 0 && out.PendingCount == 0 && out.ReadbackErrorCount == 0
	out.FollowupRequired = out.PendingCount > 0 || out.ReadbackErrorCount > 0 || out.UnobservedTerminalCount > 0
}

func submitGraphAndMaybeWait(owner string, in jobGraphInput) (jobGraphOutput, error) {
	if len(in.Nodes) == 0 {
		return jobGraphOutput{}, fmt.Errorf("nodes cannot be empty")
	}
	if len(in.Nodes) > config.Get().JobMaxBatch {
		return jobGraphOutput{}, fmt.Errorf("graph too large: %d > %d", len(in.Nodes), config.Get().JobMaxBatch)
	}
	args := job.GraphSubmitArgs{GraphKey: in.GraphKey, Nodes: make([]job.GraphNodeArgs, 0, len(in.Nodes))}
	for _, node := range in.Nodes {
		args.Nodes = append(args.Nodes, job.GraphNodeArgs{
			NodeID: node.NodeID, Command: node.Command, Cwd: node.Cwd, Class: node.Class,
			SupersedeKey: node.SupersedeKey, DependsOn: node.DependsOn, LockKeys: node.LockKeys,
			Timeout: time.Duration(node.TimeoutSeconds) * time.Second,
		})
	}
	submitted, err := job.SubmitGraph(owner, args)
	if err != nil {
		return jobGraphOutput{}, err
	}
	out := jobGraphOutput{GraphID: submitted.GraphID, GraphKey: submitted.GraphKey, Nodes: make([]jobGraphNodeOutput, 0, len(submitted.Nodes))}
	for _, node := range submitted.Nodes {
		out.Nodes = append(out.Nodes, jobGraphNodeOutput{NodeID: node.NodeID, Job: node.Job})
	}
	if in.Async {
		classifyGraphOutput(&out)
		return out, nil
	}

	deadline := time.Now().Add(jobResultWait(in.WaitMs))
	maxBytes := aggregateMaxBytesPerJob(in.MaxBytesPerJob, len(out.Nodes))
	for i := range out.Nodes {
		n := &out.Nodes[i]
		res, err := waitResultToDeadline(owner, n.Job.JobID, maxBytes, deadline)
		if err != nil {
			n.Error = err.Error()
			continue
		}
		n.Job = res.Snapshot
		n.Output = res.Output
		n.Truncated = res.Truncated
	}
	classifyGraphOutput(&out)
	return out, nil
}

func drainJobs(owner string, in jobDrainInput) (jobDrainOutput, error) {
	if len(in.JobIDs) > config.Get().JobMaxBatch {
		return jobDrainOutput{}, fmt.Errorf("too many job_ids: %d > %d", len(in.JobIDs), config.Get().JobMaxBatch)
	}

	selected := make([]job.Snapshot, 0)
	if len(in.JobIDs) > 0 {
		selected = make([]job.Snapshot, 0, len(in.JobIDs))
		for _, id := range in.JobIDs {
			snap, err := job.Status(owner, id)
			if err != nil {
				return jobDrainOutput{}, err
			}
			selected = append(selected, snap)
		}
	} else {
		jobs, _ := job.List(owner)
		limit := config.Get().JobMaxBatch
		for i := len(jobs) - 1; i >= 0 && len(selected) < limit; i-- {
			if jobs[i].Observed {
				continue
			}
			selected = append(selected, jobs[i])
		}
	}

	out := jobDrainOutput{Selected: len(selected), Results: make([]jobDrainItemOutput, 0, len(selected))}
	if len(selected) == 0 {
		out.AllTerminal = true
		out.RemainingUnobservedTerminalCount = job.UnobservedTerminalCount(owner)
		out.FollowupRequired = out.RemainingUnobservedTerminalCount > 0
		return out, nil
	}

	deadline := time.Now().Add(jobResultWait(in.WaitMs))
	maxBytes := aggregateMaxBytesPerJob(in.MaxBytesPerJob, len(selected))
	for _, snap := range selected {
		item := jobDrainItemOutput{Job: snap}
		res, err := waitResultToDeadline(owner, snap.JobID, maxBytes, deadline)
		if err != nil {
			item.Error = err.Error()
			out.ReadbackErrorCount++
			out.Results = append(out.Results, item)
			continue
		}
		item.Job = res.Snapshot
		item.Output = res.Output
		item.Truncated = res.Truncated
		if terminalJobState(item.Job.State) {
			out.TerminalCount++
			if failedTerminalState(item.Job.State) {
				out.FailedCount++
			}
		} else {
			out.PendingCount++
			out.PendingJobIDs = append(out.PendingJobIDs, item.Job.JobID)
		}
		out.Results = append(out.Results, item)
	}
	out.RemainingUnobservedTerminalCount = job.UnobservedTerminalCount(owner)
	out.AllTerminal = out.PendingCount == 0 && out.ReadbackErrorCount == 0
	out.FollowupRequired = out.PendingCount > 0 || out.ReadbackErrorCount > 0 || out.RemainingUnobservedTerminalCount > 0
	return out, nil
}
