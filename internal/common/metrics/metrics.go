package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	KafkaConsumedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_kafka_consumed_total",
		Help: "Kafka consumed messages by topic and status.",
	}, []string{"topic", "status"})

	KafkaPublishedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_kafka_published_total",
		Help: "Kafka published messages by topic and status.",
	}, []string{"topic", "status"})

	HTTPRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_http_requests_total",
		Help: "HTTP requests by route, method and status.",
	}, []string{"route", "method", "status"})

	BillingPaymentsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "vpn_platform_billing_payments_total",
		Help: "Billing payment outcomes.",
	}, []string{"outcome"})

	KafkaHandlerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vpn_platform_kafka_handler_duration_seconds",
		Help:    "Kafka handler duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"topic"})
)

func init() {
	prometheus.MustRegister(KafkaConsumedTotal, KafkaPublishedTotal, HTTPRequestsTotal, BillingPaymentsTotal, KafkaHandlerDuration)
}

func Handler() http.Handler { return promhttp.Handler() }

func StartServer(ctx context.Context, defaultAddr string) {
	addr := strings.TrimSpace(os.Getenv("METRICS_ADDR"))
	if addr == "" {
		addr = defaultAddr
	}
	if addr == "" {
		addr = ":9090"
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	go func() {
		slog.Info("metrics server started", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server stopped", "err", err)
		}
	}()
}
