package provisioning_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/pointlist"
	"nexus-gateway/internal/provisioning"
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func TestFileClient_ServesCSVSnapshot(t *testing.T) {
	const csv = `point_id,writable,unit,object_type_bacnet,instance_no_bacnet
SOS-PT-001,false,C,analogInput,1
SOS-PT-010,true,,binaryOutput,2001
`
	path := writeFile(t, t.TempDir(), "pl.csv", csv)
	c := provisioning.NewFileClient(path, "bacnet-01")

	entries, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "analogInput,1", entries[0].LocalID)
	assert.Equal(t, "SOS-PT-001", entries[0].PointID)
	assert.True(t, entries[1].Writable)
}

func TestFileClient_ServesJSONSnapshot(t *testing.T) {
	const j = `[{"connector_id":"bacnet-01","protocol":"bacnet","local_id":"analogInput,1","point_id":"SOS-PT-001"}]`
	path := writeFile(t, t.TempDir(), "pl.json", j)
	c := provisioning.NewFileClient(path, "bacnet-01")

	entries, err := c.Snapshot(context.Background())
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "SOS-PT-001", entries[0].PointID)
}

func TestFileClient_VersionTokenStableThenChangesOnEdit(t *testing.T) {
	dir := t.TempDir()
	path := writeFile(t, dir, "pl.csv", "point_id,object_type_bacnet,instance_no_bacnet\nSOS-PT-001,analogInput,1\n")
	c := provisioning.NewFileClient(path, "bacnet-01")
	ctx := context.Background()

	v1, err := c.VersionToken(ctx)
	require.NoError(t, err)
	v1b, err := c.VersionToken(ctx)
	require.NoError(t, err)
	assert.Equal(t, v1, v1b, "token is stable while the file is unchanged")

	writeFile(t, dir, "pl.csv", "point_id,object_type_bacnet,instance_no_bacnet\nSOS-PT-001,analogInput,2\n")
	v2, err := c.VersionToken(ctx)
	require.NoError(t, err)
	assert.NotEqual(t, v1, v2, "token changes when the file content changes")
}

func TestFileClient_MissingFileErrors(t *testing.T) {
	c := provisioning.NewFileClient("/nonexistent/pl.csv", "bacnet-01")
	_, err := c.Snapshot(context.Background())
	assert.Error(t, err)
	_, err = c.VersionToken(context.Background())
	assert.Error(t, err)
}

// FileClient must satisfy the provisioning.Client interface so the existing
// sync loop (ADR-0003: provisioning snapshot overrides any bootstrap) can use it.
var _ provisioning.Client = (*provisioning.FileClient)(nil)

func TestFileClient_ImplementsClient(t *testing.T) {
	var c provisioning.Client = provisioning.NewFileClient("x.csv", "bacnet-01")
	_ = c
	_ = []pointlist.Entry(nil)
}
