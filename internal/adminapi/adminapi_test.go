package adminapi_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/adminapi"
	"nexus-gateway/internal/catalog"
	"nexus-gateway/internal/lifecycle"
	"nexus-gateway/internal/pointlist"
)

// ── helpers ──────────────────────────────────────────────────────────────────

type testFixture struct {
	privKey    *rsa.PrivateKey
	jwksServer *httptest.Server
	srv        *adminapi.Server
	apiServer  *httptest.Server
}

func newFixture(t *testing.T) *testFixture {
	t.Helper()

	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pub, err := jwk.PublicKeyOf(privKey)
	require.NoError(t, err)
	require.NoError(t, pub.Set(jwk.KeyIDKey, "test-key"))
	require.NoError(t, pub.Set(jwk.AlgorithmKey, jwa.RS256))

	set := jwk.NewSet()
	require.NoError(t, set.AddKey(pub))

	jwksServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(set) //nolint:errcheck
	}))
	t.Cleanup(jwksServer.Close)

	mgr := &mockManager{}
	mon := &mockMonitor{}
	srv := adminapi.New(mgr, mon, jwksServer.URL, "nexus-gateway", "test-issuer")
	t.Cleanup(srv.Shutdown)
	apiServer := httptest.NewServer(srv)
	t.Cleanup(apiServer.Close)

	return &testFixture{
		privKey:    privKey,
		jwksServer: jwksServer,
		srv:        srv,
		apiServer:  apiServer,
	}
}

func (f *testFixture) signToken(t *testing.T, roles []string, expiry time.Time) string {
	t.Helper()
	return signToken(t, f.privKey, "test-issuer", "nexus-gateway", roles, expiry)
}

// signToken builds and signs a JWT with configurable issuer and audience.
func signToken(t *testing.T, privKey *rsa.PrivateKey, issuer, audience string, roles []string, expiry time.Time) string {
	t.Helper()
	b := jwt.NewBuilder().
		Issuer(issuer).
		Audience([]string{audience}).
		Expiration(expiry).
		Claim("realm_access", map[string]any{"roles": roles})
	tok, err := b.Build()
	require.NoError(t, err)
	priv, err := jwk.FromRaw(privKey)
	require.NoError(t, err)
	require.NoError(t, priv.Set(jwk.KeyIDKey, "test-key"))
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256, priv))
	require.NoError(t, err)
	return string(signed)
}

func (f *testFixture) get(path, token string) *http.Response {
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, f.apiServer.URL+path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	return resp
}

func (f *testFixture) post(path, token string) *http.Response {
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, f.apiServer.URL+path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	return resp
}

// ── mocks ──────────────────────────────────────────────────────────────────

type mockManager struct {
	lastAction string
	lastID     string
	err        error
}

func (m *mockManager) Start(_ context.Context, id string) error {
	m.lastAction, m.lastID = "start", id
	return m.err
}
func (m *mockManager) Stop(_ context.Context, id string) error {
	m.lastAction, m.lastID = "stop", id
	return m.err
}
func (m *mockManager) Restart(_ context.Context, id string) error {
	m.lastAction, m.lastID = "restart", id
	return m.err
}
func (m *mockManager) Upgrade(_ context.Context, id, _ string) error {
	m.lastAction, m.lastID = "upgrade", id
	return m.err
}
func (m *mockManager) Rollback(_ context.Context, id string) error {
	m.lastAction, m.lastID = "rollback", id
	return m.err
}

type mockPointListSource struct {
	entries []pointlist.Entry
}

func (m *mockPointListSource) Snapshot() []pointlist.Entry { return m.entries }


type mockCatalogSource struct {
	manifests  []catalog.Manifest
	lastUpdate string
	err        error
}

func (m *mockCatalogSource) ListAll(_ context.Context) ([]catalog.Manifest, error) {
	return m.manifests, m.err
}
func (m *mockCatalogSource) Update(_ context.Context, id string) error {
	m.lastUpdate = id
	return m.err
}

type mockInstaller struct {
	lastInstall string
	err         error
}

func (m *mockInstaller) Install(_ context.Context, name string) error {
	m.lastInstall = name
	return m.err
}

type mockMonitor struct{}

func (m *mockMonitor) Snapshot(_ context.Context) lifecycle.GatewayHealth {
	return lifecycle.GatewayHealth{
		UptimeSeconds: 42.0,
		GoRoutines:    5,
		MemAllocMB:    1.5,
		Connectors: []lifecycle.ConnectorHealth{
			{ID: "mqtt-01", Running: true},
		},
	}
}

// ── auth tests ────────────────────────────────────────────────────────────

func TestAuth_NoToken_Returns401(t *testing.T) {
	f := newFixture(t)
	resp := f.get("/connectors", "")
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAuth_ExpiredToken_Returns401(t *testing.T) {
	f := newFixture(t)
	tok := f.signToken(t, []string{adminapi.RoleViewer}, time.Now().Add(-1*time.Hour))
	resp := f.get("/connectors", tok)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAuth_WrongAudience_Returns401(t *testing.T) {
	f := newFixture(t)
	tok := signToken(t, f.privKey, "test-issuer", "wrong-audience", []string{adminapi.RoleViewer}, time.Now().Add(1*time.Hour))
	resp := f.get("/connectors", tok)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAuth_WrongIssuer_Returns401(t *testing.T) {
	f := newFixture(t)
	tok := signToken(t, f.privKey, "evil-realm", "nexus-gateway", []string{adminapi.RoleViewer}, time.Now().Add(1*time.Hour))
	resp := f.get("/connectors", tok)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAuth_ViewerCanListConnectors(t *testing.T) {
	f := newFixture(t)
	tok := f.signToken(t, []string{adminapi.RoleViewer}, time.Now().Add(1*time.Hour))
	resp := f.get("/connectors", tok)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAuth_OperatorCanListConnectors(t *testing.T) {
	f := newFixture(t)
	tok := f.signToken(t, []string{adminapi.RoleOperator}, time.Now().Add(1*time.Hour))
	resp := f.get("/connectors", tok)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAuth_ViewerCannotRestart_Returns403(t *testing.T) {
	f := newFixture(t)
	tok := f.signToken(t, []string{adminapi.RoleViewer}, time.Now().Add(1*time.Hour))
	resp := f.post("/connectors/mqtt-01/restart", tok)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestAuth_OperatorCanRestart(t *testing.T) {
	f := newFixture(t)
	tok := f.signToken(t, []string{adminapi.RoleOperator}, time.Now().Add(1*time.Hour))
	resp := f.post("/connectors/mqtt-01/restart", tok)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

// ── endpoint tests ───────────────────────────────────────────────────────

func TestHealth_NoAuthRequired(t *testing.T) {
	f := newFixture(t)
	resp := f.get("/health", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var h lifecycle.GatewayHealth
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&h))
	assert.Greater(t, h.UptimeSeconds, 0.0)
}

func TestConnectors_ReturnsConnectorList(t *testing.T) {
	f := newFixture(t)
	tok := f.signToken(t, []string{adminapi.RoleViewer}, time.Now().Add(1*time.Hour))
	resp := f.get("/connectors", tok)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var items []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&items))
	require.Len(t, items, 1)
	assert.Equal(t, "mqtt-01", items[0]["id"])
	assert.Equal(t, true, items[0]["running"])
}

func TestAction_Start(t *testing.T) {
	f := newFixture(t)
	mgr := &mockManager{}
	mon := &mockMonitor{}
	srv := adminapi.New(mgr, mon, f.jwksServer.URL, "nexus-gateway", "test-issuer")
	t.Cleanup(srv.Shutdown)
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	tok := f.signToken(t, []string{adminapi.RoleOperator}, time.Now().Add(1*time.Hour))
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, apiSrv.URL+"/connectors/mqtt-01/start", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ := http.DefaultClient.Do(req)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "start", mgr.lastAction)
	assert.Equal(t, "mqtt-01", mgr.lastID)
}

func TestAction_UnknownConnector_Returns404(t *testing.T) {
	f := newFixture(t)
	mgr := &mockManager{err: fmt.Errorf("lifecycle: connector %q: %w", "ghost", lifecycle.ErrConnectorNotFound)}
	mon := &mockMonitor{}
	srv := adminapi.New(mgr, mon, f.jwksServer.URL, "nexus-gateway", "test-issuer")
	t.Cleanup(srv.Shutdown)
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	tok := f.signToken(t, []string{adminapi.RoleOperator}, time.Now().Add(1*time.Hour))
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, apiSrv.URL+"/connectors/ghost/restart", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, _ := http.DefaultClient.Do(req)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestAction_UnknownAction_Returns400(t *testing.T) {
	f := newFixture(t)
	tok := f.signToken(t, []string{adminapi.RoleOperator}, time.Now().Add(1*time.Hour))
	resp := f.post("/connectors/mqtt-01/explode", tok)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// ── devices tests ────────────────────────────────────────────────────────────

func TestDevices_ListAll(t *testing.T) {
	src := &mockPointListSource{entries: []pointlist.Entry{
		{ConnectorID: "bacnet-01", Protocol: "bacnet", LocalID: "AHU-1/sup_temp", PointID: "p-001", Unit: "Cel", DeviceRef: "ahu-01"},
		{ConnectorID: "bacnet-01", Protocol: "bacnet", LocalID: "AHU-1/fan_run", PointID: "p-002", Writable: true, DeviceRef: "ahu-01"},
	}}
	srv := adminapi.NewNoAuthWithOptions(&mockManager{}, &mockMonitor{}, adminapi.ServerOptions{PointList: src})
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	resp, err := http.Get(apiSrv.URL + "/devices")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var items []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&items))
	require.Len(t, items, 2)
	assert.Equal(t, "p-001", items[0]["point_id"])
	assert.Equal(t, "bacnet-01", items[0]["connector_id"])
	assert.Equal(t, "Cel", items[0]["unit"])
	assert.Equal(t, true, items[1]["writable"])
}

func TestDevices_NilSource_Returns404(t *testing.T) {
	srv := adminapi.NewNoAuthWithOptions(&mockManager{}, &mockMonitor{}, adminapi.ServerOptions{})
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	resp, _ := http.Get(apiSrv.URL + "/devices")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// ── telemetry tests ──────────────────────────────────────────────────────────

type mockTelemetrySource struct {
	drifts map[string]int64
	depth  int64
}

func (m *mockTelemetrySource) Drifts() map[string]int64 { return m.drifts }
func (m *mockTelemetrySource) Depth() int64              { return m.depth }

func TestTelemetry_ReturnsDriftAndDepth(t *testing.T) {
	src := &mockTelemetrySource{
		drifts: map[string]int64{"p-001": 3, "p-002": 0},
		depth:  42,
	}
	srv := adminapi.NewNoAuthWithOptions(&mockManager{}, &mockMonitor{}, adminapi.ServerOptions{Telemetry: src})
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	resp, err := http.Get(apiSrv.URL + "/telemetry")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, float64(42), body["buffer_depth"])
	drifts, ok := body["drifts"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(3), drifts["p-001"])
	assert.Equal(t, float64(0), drifts["p-002"])
}

func TestTelemetry_NilSource_Returns404(t *testing.T) {
	srv := adminapi.NewNoAuthWithOptions(&mockManager{}, &mockMonitor{}, adminapi.ServerOptions{})
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	resp, _ := http.Get(apiSrv.URL + "/telemetry")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// ── canonical constructor tests (NewServer / NewSecureServer) ─────────────────

// NewServer is the no-auth constructor: optional sources via ServerOptions,
// every endpoint open. It must register optional routes from the options.
func TestNewServer_RegistersOptionalRoutes(t *testing.T) {
	src := &mockTelemetrySource{drifts: map[string]int64{"p1": 1}, depth: 7}
	srv := adminapi.NewServer(&mockManager{}, &mockMonitor{}, adminapi.ServerOptions{Telemetry: src})
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	resp, err := http.Get(apiSrv.URL + "/telemetry")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// NewSecureServer is the JWT constructor: operator endpoints reject requests
// that carry no bearer token.
func TestNewSecureServer_RejectsUnauthenticated(t *testing.T) {
	f := newFixture(t)
	srv := adminapi.NewSecureServer(&mockManager{}, &mockMonitor{}, adminapi.ServerOptions{},
		adminapi.JWTConfig{JWKSURL: f.jwksServer.URL, Audience: "nexus-gateway", Issuer: "test-issuer"})
	t.Cleanup(srv.Shutdown)
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	resp, err := http.Get(apiSrv.URL + "/connectors")
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// ── logs tests ───────────────────────────────────────────────────────────────

type mockConnectorLogger struct {
	lines map[string][]string
	err   error
}

func (m *mockConnectorLogger) Logs(_ context.Context, id string, _ int) ([]string, error) {
	return m.lines[id], m.err
}

func TestLogs_ReturnsLinesForConnector(t *testing.T) {
	lg := &mockConnectorLogger{
		lines: map[string][]string{
			"bacnet-01": {"2026-06-15 INFO starting", "2026-06-15 WARN reconnecting"},
		},
	}
	srv := adminapi.NewNoAuthWithOptions(&mockManager{}, &mockMonitor{}, adminapi.ServerOptions{Logger: lg})
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	resp, err := http.Get(apiSrv.URL + "/logs/bacnet-01")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "bacnet-01", body["connector_id"])
	lines, ok := body["lines"].([]any)
	require.True(t, ok)
	assert.Len(t, lines, 2)
	assert.Equal(t, "2026-06-15 INFO starting", lines[0])
}

func TestLogs_UnknownConnector_Returns404(t *testing.T) {
	lg := &mockConnectorLogger{err: fmt.Errorf("lifecycle: connector %q: %w", "ghost", lifecycle.ErrConnectorNotFound)}
	srv := adminapi.NewNoAuthWithOptions(&mockManager{}, &mockMonitor{}, adminapi.ServerOptions{Logger: lg})
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	resp, _ := http.Get(apiSrv.URL + "/logs/ghost")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestLogs_NilSource_Returns404(t *testing.T) {
	srv := adminapi.NewNoAuthWithOptions(&mockManager{}, &mockMonitor{}, adminapi.ServerOptions{})
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	resp, _ := http.Get(apiSrv.URL + "/logs/bacnet-01")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// ── catalog tests ────────────────────────────────────────────────────────────

func TestCatalog_ListAll_NoAuth(t *testing.T) {
	src := &mockCatalogSource{
		manifests: []catalog.Manifest{
			{Name: "sim-connector", Version: "1.0.0", Image: "ghcr.io/org/sim-connector:v1.0.0", Digest: "sha256:abc123"},
		},
	}
	srv := adminapi.NewNoAuthWithInstaller(&mockManager{}, &mockInstaller{}, &mockMonitor{}, src)
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	resp, err := http.Get(apiSrv.URL + "/catalog")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var entries []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&entries))
	require.Len(t, entries, 1)
	assert.Equal(t, "sim-connector", entries[0]["name"])
	assert.Equal(t, "1.0.0", entries[0]["version"])
	assert.Equal(t, "sha256:abc123", entries[0]["digest"])
}

func TestCatalog_NilSource_Returns404(t *testing.T) {
	srv := adminapi.NewNoAuthWithInstaller(&mockManager{}, nil, &mockMonitor{})
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	resp, _ := http.Get(apiSrv.URL + "/catalog")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCatalog_UpdateAction_CallsCatalogSource(t *testing.T) {
	src := &mockCatalogSource{}
	srv := adminapi.NewNoAuthWithInstaller(&mockManager{}, &mockInstaller{}, &mockMonitor{}, src)
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, apiSrv.URL+"/connectors/sim-connector/update", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "sim-connector", src.lastUpdate)
}

func TestCatalog_JWTPath_ListAll(t *testing.T) {
	f := newFixture(t)
	src := &mockCatalogSource{
		manifests: []catalog.Manifest{
			{Name: "bacnet-connector", Version: "2.0.0", Image: "ghcr.io/org/bacnet:v2.0.0", Digest: "sha256:deadbeef"},
		},
	}
	mgr := &mockManager{}
	mon := &mockMonitor{}
	srv := adminapi.NewWithCatalog(mgr, &mockInstaller{}, src, mon, f.jwksServer.URL, "nexus-gateway", "test-issuer")
	t.Cleanup(srv.Shutdown)
	apiSrv := httptest.NewServer(srv)
	t.Cleanup(apiSrv.Close)

	tok := f.signToken(t, []string{adminapi.RoleViewer}, time.Now().Add(1*time.Hour))
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, apiSrv.URL+"/catalog", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var entries []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&entries))
	require.Len(t, entries, 1)
	assert.Equal(t, "bacnet-connector", entries[0]["name"])
}

func TestMetrics_NoAuthRequired(t *testing.T) {
	f := newFixture(t)
	resp := f.get("/metrics", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	bodyBytes, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(bodyBytes), "gateway_uptime_seconds")
	assert.Contains(t, string(bodyBytes), "gateway_goroutines")
	assert.Contains(t, string(bodyBytes), "normalizer_invalid_total")
	assert.Contains(t, string(bodyBytes), "normalizer_unresolved_total")
}
