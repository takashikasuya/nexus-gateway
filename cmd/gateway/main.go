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

	"net/http"

	dockerclient "github.com/docker/docker/client"

	"nexus-gateway/connector/sim"
	"nexus-gateway/internal/adminapi"
	"nexus-gateway/internal/dispatch"
	"nexus-gateway/internal/egress"
	"nexus-gateway/internal/lifecycle"
	"nexus-gateway/internal/normalizer"
	"nexus-gateway/internal/pointlist"
	"nexus-gateway/internal/pointsync"
	"nexus-gateway/internal/provisioning"
	"nexus-gateway/internal/storeforward"
	"nexus-gateway/internal/transport"
	"nexus-gateway/internal/uplink"
)

func main() {
	natsURL := flag.String("nats", envOrDefault("NATS_URL", nats.DefaultURL), "NATS URL")
	bosAddr := flag.String("bos", envOrDefault("BOS_ADDR", "localhost:50051"), "Building OS gRPC address")
	gatewayID := flag.String("gateway-id", envOrDefault("GATEWAY_ID", "gw-001"), "Gateway ID")
	adminAddr := flag.String("admin-addr", envOrDefault("ADMIN_ADDR", ":8080"), "Admin API listen address")
	jwksURL := flag.String("admin-jwks-url", envOrDefault("KEYCLOAK_JWKS_URL", ""), "Keycloak JWKS URL (empty = auth disabled)")
	adminAudience := flag.String("admin-audience", envOrDefault("KEYCLOAK_AUDIENCE", "account"), "Expected JWT audience")
	adminIssuer := flag.String("admin-issuer", envOrDefault("KEYCLOAK_ISSUER", ""), "Expected JWT issuer")
	plFile := flag.String("point-list", envOrDefault("POINT_LIST_FILE", "fixtures/point_list.json"), "Bootstrap fixture point list (used when --provisioning-url is empty)")
	plPersist := flag.String("point-list-persist", envOrDefault("POINT_LIST_PERSIST", "data/point_list.json"), "Path to persist the synced point list")
	provURL := flag.String("provisioning-url", envOrDefault("PROVISIONING_URL", ""), "Provisioning API base URL (empty = fixture only)")
	sfDB := flag.String("sf-db", envOrDefault("SF_DB", "data/storeforward.db"), "Store-and-Forward SQLite database path")
	sfCap := flag.Int("sf-cap", 100_000, "Store-and-Forward ring buffer capacity (frames)")
	bosInsecure := flag.Bool("bos-insecure", envOrDefault("BOS_INSECURE", "") == "true", "Dial Building OS over plaintext h2c (no TLS) — dev/CI only (ADR-0007)")
	bosCA := flag.String("bos-ca", envOrDefault("BOS_CA_FILE", ""), "PEM CA bundle to verify the Building OS server cert (empty = system roots)")
	bosCert := flag.String("bos-cert", envOrDefault("BOS_CERT_FILE", ""), "Client certificate for mTLS to Building OS (CN/SAN = gateway_id)")
	bosKey := flag.String("bos-key", envOrDefault("BOS_KEY_FILE", ""), "Client private key for mTLS to Building OS")
	bosServerName := flag.String("bos-servername", envOrDefault("BOS_SERVER_NAME", ""), "Override the server name verified in the Building OS cert")
	flag.Parse()

	// Build the gRPC transport credentials for both Building OS links (ADR-0007).
	bosCreds, err := transport.ClientCredentials(transport.Config{
		Insecure:   *bosInsecure,
		CAFile:     *bosCA,
		CertFile:   *bosCert,
		KeyFile:    *bosKey,
		ServerName: *bosServerName,
	})
	if err != nil {
		slog.Error("Building OS transport credentials", "err", err)
		os.Exit(1)
	}
	if *bosInsecure {
		slog.Warn("Building OS link is plaintext h2c (--bos-insecure) — dev/CI only")
	}

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
	ul, err := uplink.NewIngress(ctx, *bosAddr, *gatewayID, buf, uplink.DefaultConfig, bosCreds)
	if err != nil {
		slog.Error("uplink init failed", "err", err)
		os.Exit(1)
	}
	go ul.Run(ctx)

	// Start Egress agent (control path, ADR-0004)
	d := dispatch.New(nc, resolver, 5*time.Second)
	egressAgent := egress.New(nc, *bosAddr, *gatewayID, d, bosCreds)
	go egressAgent.Run(ctx)

	// Start Admin API
	connRegistry := lifecycle.NewRegistry()
	docker, dockerErr := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if dockerErr != nil {
		slog.Warn("admin: Docker client unavailable — lifecycle actions disabled", "err", dockerErr)
	}
	var dockerCC lifecycle.ContainerClient
	if docker != nil {
		dockerCC = docker
	}
	connMgr := lifecycle.NewManager(dockerCC, connRegistry)
	healthMon := lifecycle.NewHealthMonitor(dockerCC, connRegistry)
	var adminSrv *adminapi.Server
	if *jwksURL != "" {
		adminSrv = adminapi.New(connMgr, healthMon, *jwksURL, *adminAudience, *adminIssuer)
	} else {
		slog.Warn("admin: JWT auth disabled — set KEYCLOAK_JWKS_URL before exposing this port")
		adminSrv = adminapi.NewNoAuth(connMgr, healthMon)
	}
	httpSrv := &http.Server{Addr: *adminAddr, Handler: adminSrv}
	go func() {
		slog.Info("admin: listening", "addr", *adminAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("admin: server error", "err", err)
		}
	}()

	// Register sim connector in the lifecycle registry so the Admin UI can show it.
	// The sim connector runs as an in-process goroutine (no container), so ContainerID
	// stays empty and Docker inspection is skipped in Snapshot.
	connRegistry.Register(lifecycle.ConnectorSpec{ID: "sim-01", Image: "sim:dev"})
	connRegistry.SetRunning("sim-01", "", true)

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
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		slog.Warn("admin: shutdown error", "err", err)
	}
	if adminSrv != nil {
		adminSrv.Shutdown()
	}
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
