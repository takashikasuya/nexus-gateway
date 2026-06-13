package adminapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"nexus-gateway/internal/lifecycle"
)

const (
	RoleOperator = "gateway-operator"
	RoleViewer   = "gateway-viewer"
)

// ConnectorManager is the lifecycle.Manager subset the Server needs.
type ConnectorManager interface {
	Start(ctx context.Context, id string) error
	Stop(ctx context.Context, id string) error
	Restart(ctx context.Context, id string) error
	Upgrade(ctx context.Context, id, newImage string) error
}

// HealthSnapshotter produces gateway health snapshots.
type HealthSnapshotter interface {
	Snapshot(ctx context.Context) lifecycle.GatewayHealth
}

// Server is the Admin HTTP API server.
type Server struct {
	mux     *http.ServeMux
	auth    *JWTMiddleware
	mgr     ConnectorManager
	monitor HealthSnapshotter
}

// New creates a Server. jwksURL is fetched for JWKS key validation; audience
// and issuer are enforced on every token.
func New(mgr ConnectorManager, monitor HealthSnapshotter, jwksURL, audience, issuer string) *Server {
	return newServer(mgr, monitor, newURLKeyFetcher(jwksURL), audience, issuer)
}

func newServer(mgr ConnectorManager, monitor HealthSnapshotter, keys KeyFetcher, audience, issuer string) *Server {
	s := &Server{
		mux:     http.NewServeMux(),
		auth:    &JWTMiddleware{keys: keys, audience: audience, issuer: issuer},
		mgr:     mgr,
		monitor: monitor,
	}
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /connectors", s.auth.require(RoleViewer, s.handleListConnectors))
	s.mux.HandleFunc("POST /connectors/{id}/{action}", s.auth.require(RoleOperator, s.handleAction))
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.monitor.Snapshot(r.Context()))
}

type connectorItem struct {
	ID      string `json:"id"`
	Running bool   `json:"running"`
}

func (s *Server) handleListConnectors(w http.ResponseWriter, r *http.Request) {
	h := s.monitor.Snapshot(r.Context())
	items := make([]connectorItem, 0, len(h.Connectors))
	for _, c := range h.Connectors {
		items = append(items, connectorItem{ID: c.ID, Running: c.Running})
	}
	writeJSON(w, items)
}

func (s *Server) handleAction(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	action := r.PathValue("action")

	var err error
	switch action {
	case "start":
		err = s.mgr.Start(r.Context(), id)
	case "stop":
		err = s.mgr.Stop(r.Context(), id)
	case "restart":
		err = s.mgr.Restart(r.Context(), id)
	case "upgrade":
		newImage := r.URL.Query().Get("image")
		if newImage == "" {
			http.Error(w, "upgrade requires ?image=<ref>", http.StatusBadRequest)
			return
		}
		err = s.mgr.Upgrade(r.Context(), id, newImage)
	default:
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}

	if err != nil {
		if strings.Contains(err.Error(), "not in registry") {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	h := s.monitor.Snapshot(r.Context())
	running := 0
	for _, c := range h.Connectors {
		if c.Running {
			running++
		}
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "gateway_uptime_seconds %g\n", h.UptimeSeconds)
	fmt.Fprintf(w, "gateway_goroutines %d\n", h.GoRoutines)
	fmt.Fprintf(w, "gateway_mem_alloc_mb %g\n", h.MemAllocMB)
	fmt.Fprintf(w, "gateway_connectors_total %d\n", len(h.Connectors))
	fmt.Fprintf(w, "gateway_connectors_running %d\n", running)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
