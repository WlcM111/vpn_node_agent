package vpn_node_agent

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	NodeID            string
	ServerKey         string
	KafkaBrokers      []string
	XrayAPIAddr       string
	ApplyMode         string
	StatePath         string
	HeartbeatInterval time.Duration
	ApplyTimeout      time.Duration
	AgentVersion      string
}

func LoadConfigFromEnv() (Config, error) {
	cfg := Config{
		NodeID:            strings.TrimSpace(os.Getenv("NODE_ID")),
		ServerKey:         strings.TrimSpace(os.Getenv("SERVER_KEY")),
		XrayAPIAddr:       strings.TrimSpace(os.Getenv("XRAY_API_ADDR")),
		ApplyMode:         strings.ToLower(strings.TrimSpace(os.Getenv("XRAY_APPLY_MODE"))),
		StatePath:         strings.TrimSpace(os.Getenv("NODE_AGENT_STATE_PATH")),
		HeartbeatInterval: parseDurationEnv("NODE_AGENT_HEARTBEAT_INTERVAL", 30*time.Second),
		ApplyTimeout:      parseDurationEnv("NODE_AGENT_APPLY_TIMEOUT", 10*time.Second),
		AgentVersion:      "vpn-node-agent/1.0.0",
	}
	if cfg.ApplyMode == "" {
		cfg.ApplyMode = "api"
	}
	if cfg.StatePath == "" {
		cfg.StatePath = "/var/lib/vpn-node-agent/state.json"
	}

	brokersEnv := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if brokersEnv != "" {
		parts := strings.Split(brokersEnv, ",")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				cfg.KafkaBrokers = append(cfg.KafkaBrokers, p)
			}
		}
	}

	if cfg.NodeID == "" {
		return cfg, fmt.Errorf("NODE_ID is required")
	}
	if cfg.ServerKey == "" {
		return cfg, fmt.Errorf("SERVER_KEY is required")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return cfg, fmt.Errorf("KAFKA_BROKERS is required")
	}
	if cfg.ApplyMode != "api" && cfg.ApplyMode != "dry-run" {
		return cfg, fmt.Errorf("XRAY_APPLY_MODE must be api or dry-run")
	}
	if cfg.ApplyMode == "api" && cfg.XrayAPIAddr == "" {
		return cfg, fmt.Errorf("XRAY_API_ADDR is required when XRAY_APPLY_MODE=api")
	}
	return cfg, nil
}

func parseDurationEnv(key string, fallback time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if sec, err := strconv.Atoi(v); err == nil && sec > 0 {
		return time.Duration(sec) * time.Second
	}
	return fallback
}
