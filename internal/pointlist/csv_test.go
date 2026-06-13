package pointlist_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"nexus-gateway/internal/pointlist"
)

// A trimmed slice of the building-os-e2e-test mvp-pointlist.csv shape.
const sampleCSV = `gateway_id,device_id,point_id,point_name,writable,unit,description,local_id,object_type_bacnet,instance_no_bacnet
GW-SOS-001,SOS-DEV-001,SOS-PT-001,Entrance Temperature,false,C,temp,L-001,analogInput,1
GW-SOS-001,SOS-DEV-001,SOS-PT-002,Entrance Humidity,false,%,humidity,L-002,analogInput,2
GW-SOS-001,SOS-DEV-002,SOS-PT-010,Damper Command,true,,cmd,L-010,binaryOutput,2001
`

func TestLoadCSV_ProjectsBACnetNativeAddress(t *testing.T) {
	entries, err := pointlist.LoadCSV(strings.NewReader(sampleCSV), "bacnet-01")
	require.NoError(t, err)
	require.Len(t, entries, 3)

	e := entries[0]
	assert.Equal(t, "bacnet-01", e.ConnectorID)
	assert.Equal(t, "bacnet", e.Protocol)
	// Native address is object_type_bacnet + instance_no_bacnet, NOT the SBCO local_id column.
	assert.Equal(t, "analogInput,1", e.LocalID)
	assert.Equal(t, "SOS-PT-001", e.PointID)
	assert.Equal(t, "C", e.Unit)
	assert.False(t, e.Writable)
	assert.Equal(t, "SOS-DEV-001", e.DeviceRef)
}

func TestLoadCSV_ParsesWritable(t *testing.T) {
	entries, err := pointlist.LoadCSV(strings.NewReader(sampleCSV), "bacnet-01")
	require.NoError(t, err)
	assert.Equal(t, "binaryOutput,2001", entries[2].LocalID)
	assert.True(t, entries[2].Writable, "writable=true must be parsed")
}

func TestLoadCSV_SkipsRowsMissingBACnetAddress(t *testing.T) {
	const csv = `point_id,writable,unit,object_type_bacnet,instance_no_bacnet
SOS-PT-001,false,C,analogInput,1
SOS-PT-OPCUA,false,C,,
`
	entries, err := pointlist.LoadCSV(strings.NewReader(csv), "bacnet-01")
	require.NoError(t, err)
	require.Len(t, entries, 1, "rows without a BACnet object type/instance are skipped")
	assert.Equal(t, "analogInput,1", entries[0].LocalID)
}

func TestLoadCSV_RequiresPointIDColumn(t *testing.T) {
	const csv = `object_type_bacnet,instance_no_bacnet
analogInput,1
`
	_, err := pointlist.LoadCSV(strings.NewReader(csv), "bacnet-01")
	require.Error(t, err, "missing point_id column must error")
}

func TestLoadCSV_ToleratesColumnReordering(t *testing.T) {
	const csv = `instance_no_bacnet,object_type_bacnet,point_id,writable
7,analogValue,SOS-PT-099,true
`
	entries, err := pointlist.LoadCSV(strings.NewReader(csv), "bacnet-01")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "analogValue,7", entries[0].LocalID)
	assert.Equal(t, "SOS-PT-099", entries[0].PointID)
	assert.True(t, entries[0].Writable)
}
