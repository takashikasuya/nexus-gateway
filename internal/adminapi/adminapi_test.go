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
	"nexus-gateway/internal/lifecycle"
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

func TestMetrics_NoAuthRequired(t *testing.T) {
	f := newFixture(t)
	resp := f.get("/metrics", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	bodyBytes, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(bodyBytes), "gateway_uptime_seconds")
	assert.Contains(t, string(bodyBytes), "gateway_goroutines")
}
