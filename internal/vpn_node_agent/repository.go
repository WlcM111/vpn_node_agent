package vpn_node_agent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type StateRepository struct {
	path string
	mu   sync.Mutex
}

func NewStateRepository(path string) *StateRepository {
	return &StateRepository{path: path}
}

func (r *StateRepository) Load(nodeID string) (*AgentState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state := &AgentState{Version: currentStateVersion, NodeID: nodeID, Profiles: map[string]AppliedProfile{}, UpdatedAt: time.Now().UTC()}
	data, err := os.ReadFile(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return nil, fmt.Errorf("read state: %w", err)
	}
	if len(data) == 0 {
		return state, nil
	}
	if err := json.Unmarshal(data, state); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	if state.Version == 0 {
		state.Version = 1
	}
	if state.Version > currentStateVersion {
		backup := r.path + ".unsupported-version-" + time.Now().UTC().Format("20060102150405")
		_ = os.WriteFile(backup, data, 0o600)
		return nil, fmt.Errorf("unsupported state version %d (max known %d), backup saved to %s", state.Version, currentStateVersion, backup)
	}
	// Миграция v1 → v2: добавилось поле SeenCommands. Просто инициализируем пустой map.
	if state.Profiles == nil {
		state.Profiles = map[string]AppliedProfile{}
	}
	if state.SeenCommands == nil {
		state.SeenCommands = map[string]time.Time{}
	}
	state.NodeID = nodeID
	state.Version = currentStateVersion
	return state, nil
}

func (r *StateRepository) Save(state *AgentState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if state.Profiles == nil {
		state.Profiles = map[string]AppliedProfile{}
	}
	if state.SeenCommands == nil {
		state.SeenCommands = map[string]time.Time{}
	}
	state.Version = currentStateVersion
	state.UpdatedAt = time.Now().UTC()
	if err := os.MkdirAll(filepath.Dir(r.path), 0o750); err != nil {
		return fmt.Errorf("mkdir state dir: %w", err)
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write tmp state: %w", err)
	}
	if err := os.Rename(tmp, r.path); err != nil {
		return fmt.Errorf("rename state: %w", err)
	}
	return nil
}

func (r *StateRepository) CountEnabled(state *AgentState) int {
	if state == nil {
		return 0
	}
	cnt := 0
	for _, p := range state.Profiles {
		if p.Enabled {
			cnt++
		}
	}
	return cnt
}
