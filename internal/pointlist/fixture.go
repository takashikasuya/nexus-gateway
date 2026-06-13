package pointlist

// Entry maps one native address to one canonical PointID (ADR-0003).
type Entry struct {
	ConnectorID   string `json:"connector_id"`
	Protocol      string `json:"protocol"`
	LocalID       string `json:"local_id"`
	PointID       string `json:"point_id"`
	Unit          string `json:"unit,omitempty"`
	Writable      bool   `json:"writable,omitempty"`
	DeviceRef     string `json:"device_ref,omitempty"`
	ControlSchema string `json:"control_schema,omitempty"`
}

// Resolver resolves a native local_id to a canonical point_id.
type Resolver interface {
	Resolve(connectorID, localID string) (pointID string, ok bool)
}

// Fixture is a static resolver backed by a slice — used for the walking skeleton
// and tests. The real sync-loop resolver (EP-006) satisfies the same interface.
type Fixture struct {
	index map[string]string // "connectorID/localID" → pointID
}

func NewFixture(entries []Entry) *Fixture {
	f := &Fixture{index: make(map[string]string, len(entries))}
	for _, e := range entries {
		f.index[key(e.ConnectorID, e.LocalID)] = e.PointID
	}
	return f
}

func (f *Fixture) Resolve(connectorID, localID string) (string, bool) {
	v, ok := f.index[key(connectorID, localID)]
	return v, ok
}

func key(connectorID, localID string) string {
	return connectorID + "\x00" + localID
}
