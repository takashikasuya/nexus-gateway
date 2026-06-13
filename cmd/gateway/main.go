package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"nexus-gateway/connector/sim"
	"nexus-gateway/internal/normalizer"
	"nexus-gateway/internal/pointlist"
	"nexus-gateway/internal/pointsync"
	"nexus-gateway/internal/provisioning"
	"nexus-gateway/internal/storeforward"
	"nexus-gateway/internal/uplink"
)

func main() {
	natsURL := flag.String("nats", envOrDefault("NATS_URL", nats.DefaultURL), "NATS URL")
	bosAddr := flag.String("bos", envOrDefault("BOS_ADDR", "localhost:50051"), "Building OS gRPC address")
	gatewayID := flag.String("gateway-id", envOrDefault("GATEWAY_ID", "gw-001"), "Gateway ID")
	plFile := flag.String("point-list", envOrDefault("POINT_LIST_FILE", "fixtures/point_list.json"), "Bootstrap fixture point list (used when --provisioning-url is empty)")
	plPersist := flag.String("point-list-persist", envOrDefault("POINT_LIST_PERSIST", "data/point_list.json"), "Path to persist the synced point list")
	provURL := flag.String("provisioning-url", envOrDefault("PROVISIONING_URL", ""), "Provisioning API base URL (empty = fixture only)")
	sfDB := flag.String("sf-db", envOrDefault("SF_DB", "data/storeforward.db"), "Store-and-Forward SQLite database path")
	sfCap := flag.Int("sf-cap", 100_000, "Store-and-Forward ring buffer capacity (frames)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to NATS
	nc, err := nats.Connect(*natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		slog.Error("NATS connect failed", "err", err)
		os.Exit(1)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("JetStream init failed", "err", err)
		os.Exit(1)
	}

	// Provision EVENTS stream (ADR-0005)
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      "EVENTS",
		Subjects:  []string{"evt.>"},
		MaxAge:    48 * time.Hour,
		MaxBytes:  2 * 1024 * 1024 * 1024,
		Discard:   jetstream.DiscardOld,
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}); err != nil {
		slog.Error("EVENTS stream provision failed", "err", err)
		os.Exit(1)
	}

	// Build the live point list resolver
	resolver := pointlist.NewSynced(nil)
	if *provURL != "" {
		// Real sync loop against the provisioning API (ADR-0003)
		syncLoop := pointsync.New(
			provisioning.NewHTTPClient(*provURL),
			resolver,
			pointsync.Config{Interval: 30 * time.Second, PersistPath: *plPersist},
		)
		go syncLoop.Run(ctx)
		// Wait for initial snapshot before starting the pipeline
		waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
		defer waitCancel()
		for {
			if len(resolver.Snapshot()) > 0 || waitCtx.Err() != nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	} else {
		// Bootstrap from fixture file (dev / no provisioning API)
		entries, err := loadFixtureEntries(*plFile)
		if err != nil {
			slog.Error("load point list failed", "err", err)
			os.Exit(1)
		}
		resolver.Update(entries)
	}

	// Start Normalizer
	norm, err := normalizer.New(ctx, js, resolver, *gatewayID)
	if err != nil {
		slog.Error("normalizer init failed", "err", err)
		os.Exit(1)
	}

	// Open Store-and-Forward buffer (create parent directory if needed)
	if err := os.MkdirAll(filepath.Dir(*sfDB), 0o755); err != nil {
		slog.Error("storeforward dir create failed", "err", err)
		os.Exit(1)
	}
	buf, err := storeforward.Open(*sfDB, *sfCap)
	if err != nil {
		slog.Error("storeforward open failed", "err", err)
		os.Exit(1)
	}
	var pumpWg sync.WaitGroup
	pumpWg.Add(1)
	go func() {
		defer pumpWg.Done()
		storeforward.Pump(ctx, norm.Frames(), buf)
	}()

	// Start Ingress uplink
	ul, err := uplink.NewIngress(ctx, *bosAddr, *gatewayID, buf, uplink.DefaultConfig)
	if err != nil {
		slog.Error("uplink init failed", "err", err)
		os.Exit(1)
	}
	go ul.Run(ctx)

	// Start sim connector
	connector := sim.New("sim-01", js, 5*time.Second, []sim.Point{
		{LocalID: "sim://ahu-01/supply_air_temp", DeviceRef: "sim://ahu-01", Unit: "Cel", BaseValue: 22.0, Amplitude: 3.0},
		{LocalID: "sim://ahu-01/fan_run", DeviceRef: "sim://ahu-01", Unit: "", BaseValue: 1.0, Amplitude: 0.0},
	})
	go connector.Run(ctx)

	slog.Info("gateway started", "gateway_id", *gatewayID, "nats", *natsURL, "bos", *bosAddr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop
	slog.Info("gateway shutting down")
	cancel()
	pumpWg.Wait()
	buf.Close()
}

func loadFixtureEntries(path string) ([]pointlist.Entry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var entries []pointlist.Entry
	return entries, json.Unmarshal(data, &entries)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
