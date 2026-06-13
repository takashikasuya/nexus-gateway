package pointlist

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

// LoadCSV parses the SBCO point-list CSV (as used by building-os-e2e-test) into
// Entries for the given BACnet connector. The protocol-native address is
// projected from object_type_bacnet + instance_no_bacnet (e.g. "analogInput,1") —
// the canonical point_id comes from the point_id column (ADR-0001/ADR-0003).
//
// Columns are resolved by header name, so column order does not matter. Rows
// without a BACnet object type + instance are skipped (e.g. OPC-UA-only points).
func LoadCSV(r io.Reader, connectorID string) ([]Entry, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1 // tolerate ragged rows
	rows, err := cr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("pointlist: read CSV: %w", err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("pointlist: empty CSV")
	}

	col := map[string]int{}
	for i, name := range rows[0] {
		col[strings.TrimSpace(name)] = i
	}
	required := []string{"point_id", "object_type_bacnet", "instance_no_bacnet"}
	for _, name := range required {
		if _, ok := col[name]; !ok {
			return nil, fmt.Errorf("pointlist: CSV missing required column %q", name)
		}
	}

	get := func(row []string, name string) string {
		i, ok := col[name]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	var entries []Entry
	for _, row := range rows[1:] {
		objType := get(row, "object_type_bacnet")
		instance := get(row, "instance_no_bacnet")
		if objType == "" || instance == "" {
			continue // not a BACnet point
		}
		entries = append(entries, Entry{
			ConnectorID: connectorID,
			Protocol:    "bacnet",
			LocalID:     objType + "," + instance,
			PointID:     get(row, "point_id"),
			Unit:        get(row, "unit"),
			Writable:    strings.EqualFold(get(row, "writable"), "true"),
			DeviceRef:   get(row, "device_id"),
		})
	}
	return entries, nil
}
