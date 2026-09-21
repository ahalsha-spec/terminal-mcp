package job

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

type GraphNodeArgs struct {
	NodeID       string
	Command      string
	Cwd          string
	Class        string
	SupersedeKey string
	DependsOn    []string
	LockKeys     []string
	Timeout      time.Duration
}

type GraphSubmitArgs struct {
	GraphKey string
	Nodes    []GraphNodeArgs
}

type GraphNodeResult struct {
	NodeID string   `json:"node_id"`
	Job    Snapshot `json:"job"`
}

type GraphResult struct {
	GraphID  string            `json:"graph_id"`
	GraphKey string            `json:"graph_key,omitempty"`
	Nodes    []GraphNodeResult `json:"nodes"`
}

type graphRecord struct {
	GraphID     string
	Fingerprint string
	NodeOrder   []string
	JobIDs      map[string]string
	CreatedAt   time.Time
}

type graphFingerprintNode struct {
	NodeID       string
	Command      string
	Cwd          string
	Class        string
	SupersedeKey string
	DependsOn    []string
	LockKeys     []string
	TimeoutNanos int64
}

func normalizeGraphNodes(cfg Config, nodes []GraphNodeArgs) ([]normalizedGraphNode, string, error) {
	if len(nodes) == 0 {
		return nil, "", errors.New("graph nodes cannot be empty")
	}

	out := make([]normalizedGraphNode, 0, len(nodes))
	byID := make(map[string]struct{}, len(nodes))
	for _, raw := range nodes {
		nodeID := strings.TrimSpace(raw.NodeID)
		if nodeID == "" {
			return nil, "", errors.New("graph node_id cannot be empty")
		}
		if _, exists := byID[nodeID]; exists {
			return nil, "", fmt.Errorf("duplicate graph node_id %q", nodeID)
		}
		byID[nodeID] = struct{}{}

		cmd := strings.TrimSpace(raw.Command)
		if cmd == "" {
			return nil, "", fmt.Errorf("graph node %q command cannot be empty", nodeID)
		}
		class, err := normalizeClass(raw.Class)
		if err != nil {
			return nil, "", fmt.Errorf("graph node %q: %w", nodeID, err)
		}
		timeout := raw.Timeout
		if timeout <= 0 {
			timeout = cfg.DefaultTimeout
		}
		if timeout > cfg.MaxTimeout {
			timeout = cfg.MaxTimeout
		}
		out = append(out, normalizedGraphNode{
			NodeID:       nodeID,
			Command:      cmd,
			Cwd:          strings.TrimSpace(raw.Cwd),
			Class:        class,
			SupersedeKey: strings.TrimSpace(raw.SupersedeKey),
			DependsOn:    normalizeStringSet(raw.DependsOn),
			LockKeys:     normalizeStringSet(raw.LockKeys),
			Timeout:      timeout,
		})
	}

	indegree := make(map[string]int, len(out))
	children := make(map[string][]string, len(out))
	for _, node := range out {
		indegree[node.NodeID] = len(node.DependsOn)
		for _, dep := range node.DependsOn {
			if dep == node.NodeID {
				return nil, "", fmt.Errorf("graph node %q cannot depend on itself", node.NodeID)
			}
			if _, exists := byID[dep]; !exists {
				return nil, "", fmt.Errorf("graph node %q depends on unknown node %q", node.NodeID, dep)
			}
			children[dep] = append(children[dep], node.NodeID)
		}
	}

	ready := make([]string, 0, len(out))
	for _, node := range out {
		if indegree[node.NodeID] == 0 {
			ready = append(ready, node.NodeID)
		}
	}
	seen := 0
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		seen++
		for _, child := range children[id] {
			indegree[child]--
			if indegree[child] == 0 {
				ready = append(ready, child)
			}
		}
	}
	if seen != len(out) {
		return nil, "", errors.New("graph contains a dependency cycle")
	}

	fpNodes := make([]graphFingerprintNode, 0, len(out))
	for _, node := range out {
		fpNodes = append(fpNodes, graphFingerprintNode{
			NodeID:       node.NodeID,
			Command:      node.Command,
			Cwd:          node.Cwd,
			Class:        node.Class,
			SupersedeKey: node.SupersedeKey,
			DependsOn:    append([]string(nil), node.DependsOn...),
			LockKeys:     append([]string(nil), node.LockKeys...),
			TimeoutNanos: int64(node.Timeout),
		})
	}
	sort.Slice(fpNodes, func(i, j int) bool { return fpNodes[i].NodeID < fpNodes[j].NodeID })
	encoded, err := json.Marshal(fpNodes)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(encoded)
	return out, hex.EncodeToString(sum[:]), nil
}

type normalizedGraphNode struct {
	NodeID       string
	Command      string
	Cwd          string
	Class        string
	SupersedeKey string
	DependsOn    []string
	LockKeys     []string
	Timeout      time.Duration
}

func graphMapKey(owner, graphKey string) string {
	return owner + string(rune(31)) + graphKey
}

func (s *store) graphResultLocked(rec *graphRecord, graphKey string) GraphResult {
	out := GraphResult{
		GraphID:  rec.GraphID,
		GraphKey: graphKey,
		Nodes:    make([]GraphNodeResult, 0, len(rec.NodeOrder)),
	}
	for _, nodeID := range rec.NodeOrder {
		if j := s.jobs[rec.JobIDs[nodeID]]; j != nil {
			out.Nodes = append(out.Nodes, GraphNodeResult{NodeID: nodeID, Job: j.snapshot()})
		}
	}
	return out
}

func SubmitGraph(owner string, in GraphSubmitArgs) (GraphResult, error) {
	s := current()
	if s == nil {
		return GraphResult{}, errors.New("job scheduler not initialized")
	}
	if owner == "" {
		return GraphResult{}, errors.New("missing owner")
	}

	nodes, fingerprint, err := normalizeGraphNodes(s.cfg, in.Nodes)
	if err != nil {
		return GraphResult{}, err
	}
	graphKey := strings.TrimSpace(in.GraphKey)
	var cancels []context.CancelFunc

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return GraphResult{}, errors.New("job scheduler is shutting down")
	}

	if graphKey != "" {
		mapKey := graphMapKey(owner, graphKey)
		if rec := s.graphs[mapKey]; rec != nil {
			if rec.Fingerprint != fingerprint {
				s.mu.Unlock()
				return GraphResult{}, fmt.Errorf("graph key %q reused with different graph definition", graphKey)
			}
			result := s.graphResultLocked(rec, graphKey)
			s.mu.Unlock()
			return result, nil
		}
	}

	supersedeKeys := map[string]struct{}{}
	for _, node := range nodes {
		if node.SupersedeKey != "" {
			supersedeKeys[node.SupersedeKey] = struct{}{}
		}
	}
	plan := s.planSupersessionLocked(owner, supersedeKeys)
	queued := s.queuedCountLocked() - len(plan.queued)
	if queued+len(nodes) > s.cfg.MaxQueued {
		s.mu.Unlock()
		return GraphResult{}, fmt.Errorf("graph admission would exceed global queue after supersession plan: %d+%d > %d", queued, len(nodes), s.cfg.MaxQueued)
	}
	ownerQueued := len(s.queues[owner]) - len(plan.queued)
	if ownerQueued+len(nodes) > s.cfg.MaxQueuedPerOwner {
		s.mu.Unlock()
		return GraphResult{}, fmt.Errorf("graph admission would exceed owner queue after supersession plan: %d+%d > %d", ownerQueued, len(nodes), s.cfg.MaxQueuedPerOwner)
	}

	jobsByNode := make(map[string]*Job, len(nodes))
	order := make([]string, 0, len(nodes))
	now := time.Now()
	for _, node := range nodes {
		id := uuid.NewString()
		j := &Job{
			ID:           id,
			Owner:        owner,
			Command:      node.Command,
			Cwd:          node.Cwd,
			Class:        node.Class,
			SupersedeKey: node.SupersedeKey,
			LockKeys:     append([]string(nil), node.LockKeys...),
			State:        "queued",
			CreatedAt:    now,
			Timeout:      node.Timeout,
			OutputPath:   fmt.Sprintf("%s/jobs/%s.log", s.cfg.DataDir, id),
		}
		jobsByNode[node.NodeID] = j
		order = append(order, node.NodeID)
	}
	for _, node := range nodes {
		j := jobsByNode[node.NodeID]
		for _, depNodeID := range node.DependsOn {
			j.DependsOn = append(j.DependsOn, jobsByNode[depNodeID].ID)
		}
	}

	cancels = s.applySupersessionLocked(plan)

	if _, exists := s.queues[owner]; !exists {
		s.owners = append(s.owners, owner)
	}
	for _, nodeID := range order {
		j := jobsByNode[nodeID]
		s.jobs[j.ID] = j
		s.queues[owner] = append(s.queues[owner], j)
	}

	jobIDs := make(map[string]string, len(order))
	for _, nodeID := range order {
		jobIDs[nodeID] = jobsByNode[nodeID].ID
	}
	rec := &graphRecord{
		GraphID:     uuid.NewString(),
		Fingerprint: fingerprint,
		NodeOrder:   append([]string(nil), order...),
		JobIDs:      jobIDs,
		CreatedAt:   now,
	}
	if graphKey != "" {
		s.graphs[graphMapKey(owner, graphKey)] = rec
	}

	result := s.graphResultLocked(rec, graphKey)
	s.cond.Broadcast()
	s.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	return result, nil
}
