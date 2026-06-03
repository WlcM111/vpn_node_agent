package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	commonkafka "vpn_node_agent/internal/common/kafka"
	"vpn_node_agent/internal/common/logger"
	commonmetrics "vpn_node_agent/internal/common/metrics"
	"vpn_node_agent/internal/vpn_node_agent"
)

func main() {
	logger.Init()

	cfg, err := vpn_node_agent.LoadConfigFromEnv()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startNodeHTTP(ctx, cfg)

	producer := commonkafka.NewProducer(cfg.KafkaBrokers)
	reader := commonkafka.NewReader(cfg.KafkaBrokers, commonkafka.TopicVPNCommands, "vpn-node-agent-"+cfg.NodeID)

	var xray vpn_node_agent.XrayController
	if cfg.ApplyMode == "dry-run" {
		xray = vpn_node_agent.NewDryRunXrayController()
	} else {
		xray, err = vpn_node_agent.NewAPIXrayController(ctx, cfg.XrayAPIAddr)
		if err != nil {
			slog.Error("failed to connect xray api", "err", err)
			os.Exit(1)
		}
	}

	repo := vpn_node_agent.NewStateRepository(cfg.StatePath)

	pendingPath := strings.TrimSpace(os.Getenv("NODE_AGENT_PENDING_PATH"))
	if pendingPath == "" {
		pendingPath = filepath.Join(filepath.Dir(cfg.StatePath), "pending_events.json")
	}
	pending, err := vpn_node_agent.NewPendingQueue(pendingPath, producer)
	if err != nil {
		slog.Error("failed to init pending queue", "err", err)
		os.Exit(1)
	}

	agent, err := vpn_node_agent.NewAgent(cfg, reader, producer, xray, repo, pending)
	if err != nil {
		slog.Error("failed to create agent", "err", err)
		os.Exit(1)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := agent.Run(ctx); err != nil {
			slog.Error("agent stopped with error", "err", err)
		}
	}()

	slog.Info("vpn-node-agent started", "node_id", cfg.NodeID, "server_key", cfg.ServerKey)
	<-stop
	slog.Info("shutting down vpn-node-agent")
	cancel()
	_ = agent.Stop()
}

func startNodeHTTP(ctx context.Context, cfg vpn_node_agent.Config) {
	addr := strings.TrimSpace(os.Getenv("NODE_AGENT_HTTP_ADDR"))
	if addr == "" {
		addr = ":9091"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", commonmetrics.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("node http server stopped", "err", err)
		}
	}()
	_ = cfg
}
