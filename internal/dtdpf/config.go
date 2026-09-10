// Copyright 2026 nexus-gateway contributors
// SPDX-License-Identifier: Apache-2.0

package dtdpf

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"nexus-gateway/internal/telemetry"
)

// MetadataResolver resolves a canonical point ID to DTDPF routing metadata.
type MetadataResolver interface {
	Resolve(pointID, protocol string) (*telemetry.DTDPFMetadata, bool)
}

// PointConfig is an immutable index loaded from DTDPF pointConfig.json.
type PointConfig struct {
	byDTID map[string]telemetry.DTDPFMetadata
}

type pointConfigDocument struct {
	UpdateDatetime          string                        `json:"updateDatetime,omitempty"`
	CorrelationID           string                        `json:"correlationId,omitempty"`
	PointDataUpdateDatetime string                        `json:"pointDataUpdateDatetime,omitempty"`
	PointData               map[string][]pointConfigEntry `json:"pointData"`
}

type pointConfigEntry struct {
	DTID     string `json:"dtId"`
	Topic    string `json:"topic"`
	TypeID   *int   `json:"typeId,omitempty"`
	Type     *int   `json:"type,omitempty"`
	Protocol string `json:"protocol,omitempty"`
}

// LoadPointConfigFile loads and validates a DTDPF pointConfig.json file.
func LoadPointConfigFile(path string) (*PointConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open DTDPF point config %s: %w", path, err)
	}
	defer file.Close()
	return LoadPointConfig(file)
}

// LoadPointConfig parses and validates the DTDPF pointConfig.json contract.
func LoadPointConfig(reader io.Reader) (*PointConfig, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	var document pointConfigDocument
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode DTDPF point config: %w", err)
	}
	if len(document.PointData) == 0 {
		return nil, fmt.Errorf("DTDPF point config has no pointData")
	}

	config := &PointConfig{byDTID: make(map[string]telemetry.DTDPFMetadata)}
	for rootIDText, entries := range document.PointData {
		rootID, err := strconv.ParseInt(rootIDText, 10, 64)
		if err != nil || rootID <= 0 {
			return nil, fmt.Errorf("invalid DTDPF rootId %q", rootIDText)
		}
		for _, entry := range entries {
			entry.DTID = strings.TrimSpace(entry.DTID)
			entry.Topic = strings.TrimSpace(entry.Topic)
			entry.Protocol = strings.TrimSpace(entry.Protocol)
			if entry.DTID == "" {
				return nil, fmt.Errorf("DTDPF point under rootId %d has empty dtId", rootID)
			}
			if entry.Topic == "" {
				return nil, fmt.Errorf("DTDPF point %q has empty topic", entry.DTID)
			}
			if entry.Type != nil && *entry.Type <= 0 {
				return nil, fmt.Errorf("DTDPF point %q has invalid type %d", entry.DTID, *entry.Type)
			}
			if entry.Protocol == "" {
				entry.Protocol = "mqtt"
			}
			if _, exists := config.byDTID[entry.DTID]; exists {
				return nil, fmt.Errorf("duplicate DTDPF dtId %q", entry.DTID)
			}
			config.byDTID[entry.DTID] = telemetry.DTDPFMetadata{
				RootID:   rootID,
				DTID:     entry.DTID,
				Topic:    entry.Topic,
				Type:     entry.Type,
				Protocol: entry.Protocol,
			}
		}
	}
	return config, nil
}

// Resolve returns DTDPF metadata when dtId and protocol both match.
func (c *PointConfig) Resolve(pointID, protocol string) (*telemetry.DTDPFMetadata, bool) {
	metadata, ok := c.ResolvePoint(pointID)
	if !ok || metadata.Protocol != protocol {
		return nil, false
	}
	return metadata, true
}

// ResolvePoint returns metadata by canonical point ID without protocol filtering.
func (c *PointConfig) ResolvePoint(pointID string) (*telemetry.DTDPFMetadata, bool) {
	if c == nil {
		return nil, false
	}
	metadata, ok := c.byDTID[pointID]
	if !ok {
		return nil, false
	}
	if metadata.Type != nil {
		pointType := *metadata.Type
		metadata.Type = &pointType
	}
	return &metadata, true
}
