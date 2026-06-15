package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"nexus-gateway/internal/catalog"
	"nexus-gateway/internal/lifecycle"
	"nexus-gateway/internal/metrics"
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
	Rollback(ctx context.Context, id string) error
}

// ConnectorInstaller installs a connector from the Connector Catalog (ADR-0006).
// A nil Installer disables the /connectors/{name}/install endpoint.
type ConnectorInstaller interface {
	Install(ctx context.Context, name string) error
}

// CatalogSource provides catalog browsing and catalog-driven update operations (ADR-0006).
// A nil CatalogSource disables the /catalog and /connectors/{id}/update endpoints.
type CatalogSource interface {
	ListAll(ctx context.Context) ([]catalog.Manifest, error)
	Update(ctx context.Context, connectorID string) error
}

// HealthSnapshotter produces gateway health snapshots.
type HealthSnapshotter interface {
	Snapshot(ctx context.Context) lifecycle.GatewayHealth
}

// Server is the Admin HTTP API server.
type Server struct {
	mux       *http.ServeMux
	auth      *JWTMiddleware
	mgr       ConnectorManager
	installer ConnectorInstaller // nil if catalog is not configured
	catalog   CatalogSource      // nil if catalog browsing/update is not configured
	monitor   HealthSnapshotter
	shutdown  context.CancelFunc // stops the JWKS cache refresh goroutine
}

// New creates a Server. jwksURL is fetched for JWKS key validation; audience
// and issuer are enforced on every token. Call Shutdown() to stop background goroutines.
func New(mgr ConnectorManager, monitor HealthSnapshotter, jwksURL, audience, issuer string) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := newServer(mgr, nil, nil, monitor, newURLKeyFetcher(ctx, jwksURL), audience, issuer)
	s.shutdown = cancel
	return s
}

// NewWithCatalog creates a JWT-authenticated Server with a Catalog installer and
// source wired in. Use this in production when both auth and catalog are configured.
func NewWithCatalog(mgr ConnectorManager, installer ConnectorInstaller, catalogSrc CatalogSource, monitor HealthSnapshotter, jwksURL, audience, issuer string) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := newServer(mgr, installer, catalogSrc, monitor, newURLKeyFetcher(ctx, jwksURL), audience, issuer)
	s.shutdown = cancel
	return s
}

// NewNoAuth creates a Server with authentication disabled (dev/local use only).
func NewNoAuth(mgr ConnectorManager, monitor HealthSnapshotter) *Server {
	return NewNoAuthWithInstaller(mgr, nil, monitor)
}

// NewNoAuthWithInstaller creates a Server with auth disabled, a Catalog installer,
// and an optional CatalogSource for browsing and updates.
func NewNoAuthWithInstaller(mgr ConnectorManager, installer ConnectorInstaller, monitor HealthSnapshotter, catalogSrc ...CatalogSource) *Server {
	var src CatalogSource
	if len(catalogSrc) > 0 {
		src = catalogSrc[0]
	}
	noAuth := &JWTMiddleware{}
	s := &Server{
		mux:       http.NewServeMux(),
		auth:      noAuth,
		mgr:       mgr,
		installer: installer,
		catalog:   src,
		monitor:   monitor,
	}
	s.registerRoutes(false)
	return s
}

// Shutdown stops the JWKS cache background refresh goroutine.
func (s *Server) Shutdown() {
	if s.shutdown != nil {
		s.shutdown()
	}
}

func newServer(mgr ConnectorManager, installer ConnectorInstaller, catalogSrc CatalogSource, monitor HealthSnapshotter, keys KeyFetcher, audience, issuer string) *Server {
	s := &Server{
		mux:       http.NewServeMux(),
		auth:      &JWTMiddleware{keys: keys, audience: audience, issuer: issuer},
		mgr:       mgr,
		installer: installer,
		catalog:   catalogSrc,
		monitor:   monitor,
	}
	s.registerRoutes(true)
	return s
}

func (s *Server) registerRoutes(authenticated bool) {
	require := func(role string, h http.HandlerFunc) http.HandlerFunc {
		if !authenticated {
			return h
		}
		return s.auth.require(role, h)
	}
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /connectors", require(RoleViewer, s.handleListConnectors))
	s.mux.HandleFunc("POST /connectors/{id}/{action}", require(RoleOperator, s.handleAction))
	if s.installer != nil {
		s.mux.HandleFunc("POST /connectors/{name}/install", require(RoleOperator, s.handleInstall))
	}
	if s.catalog != nil {
		s.mux.HandleFunc("GET /catalog", require(RoleViewer, s.handleListCatalog))
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.monitor.Snapshot(r.Context()))
}

type connectorItem struct {
	ID          string `json:"id"`
	Image       string `json:"image"`
	PrevImage   string `json:"prev_image,omitempty"`
	ContainerID string `json:"container_id,omitempty"`
	Running     bool   `json:"running"`
}

func (s *Server) handleListConnectors(w http.ResponseWriter, r *http.Request) {
	h := s.monitor.Snapshot(r.Context())
	items := make([]connectorItem, 0, len(h.Connectors))
	for _, c := range h.Connectors {
		items = append(items, connectorItem{
			ID:          c.ID,
			Image:       c.Image,
			PrevImage:   c.PrevImage,
			ContainerID: c.ContainerID,
			Running:     c.Running,
		})
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
		newImage := strings.TrimSpace(r.URL.Query().Get("image"))
		if newImage == "" {
			http.Error(w, "upgrade requires ?image=<ref>", http.StatusBadRequest)
			return
		}
		err = s.mgr.Upgrade(r.Context(), id, newImage)
	case "rollback":
		err = s.mgr.Rollback(r.Context(), id)
	case "update":
		if s.catalog == nil {
			http.Error(w, "catalog not configured", http.StatusNotImplemented)
			return
		}
		err = s.catalog.Update(r.Context(), id)
	default:
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}

	if err != nil {
		if errors.Is(err, lifecycle.ErrConnectorNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleInstall(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.installer.Install(r.Context(), name); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// catalogEntry is the public representation of a catalog manifest.
type catalogEntry struct {
	Name              string   `json:"name"`
	Version           string   `json:"version"`
	Image             string   `json:"image"`
	Digest            string   `json:"digest"`
	MinGatewayVersion string   `json:"min_gateway_version"`
	SignatureRequired bool     `json:"signature_required"`
	Network           []string `json:"network,omitempty"`
	Mounts            []string `json:"mounts,omitempty"`
}

func (s *Server) handleListCatalog(w http.ResponseWriter, r *http.Request) {
	manifests, err := s.catalog.ListAll(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("catalog: %v", err), http.StatusBadGateway)
		return
	}
	entries := make([]catalogEntry, len(manifests))
	for i, m := range manifests {
		entries[i] = catalogEntry{
			Name:              m.Name,
			Version:           m.Version,
			Image:             m.Image,
			Digest:            m.Digest,
			MinGatewayVersion: m.MinGatewayVersion,
			SignatureRequired: m.SignatureRequired,
			Network:           m.Permissions.Network,
			Mounts:            m.Permissions.Mounts,
		}
	}
	writeJSON(w, entries)
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
	fmt.Fprintf(w, "# HELP normalizer_invalid_total Common Events the Normalizer could not parse.\n")
	fmt.Fprintf(w, "# TYPE normalizer_invalid_total counter\n")
	fmt.Fprintf(w, "normalizer_invalid_total %d\n", metrics.NormalizerInvalid())
	fmt.Fprintf(w, "# HELP normalizer_unresolved_total Common Events whose local_id is absent from the Point List.\n")
	fmt.Fprintf(w, "# TYPE normalizer_unresolved_total counter\n")
	fmt.Fprintf(w, "normalizer_unresolved_total{reason=\"point_list_miss\"} %d\n", metrics.NormalizerUnresolved())
}

func writeJSON(w http.ResponseWriter, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data) //nolint:errcheck
}
