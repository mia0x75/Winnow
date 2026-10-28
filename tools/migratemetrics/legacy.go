package main

import (
	"fmt"

	"github.com/liaoran123/sfsDb/engine"
	"github.com/liaoran123/sfsDb/record"
	sfsstorage "github.com/liaoran123/sfsDb/storage"
	"github.com/mia0x75/denyzer/storage"
)

// Legacy table names.
const (
	legacyTableQueryLogs = "query_logs"
	legacyTableMetrics   = "metrics"
)

// Legacy field names.
const (
	legacyFieldID           = "id"
	legacyFieldTimestamp    = "timestamp"
	legacyFieldClientIP     = "client_ip"
	legacyFieldClientName   = "client_name"
	legacyFieldDomain       = "domain"
	legacyFieldQueryType    = "query_type"
	legacyFieldResponseType = "response_type"
	legacyFieldReason       = "reason"
	legacyFieldDurationMs   = "duration_ms"

	legacyFieldDimension    = "dimension"
	legacyFieldGranularity  = "granularity"
	legacyFieldBucketStart  = "bucket_start"
	legacyFieldEntity       = "entity"
	legacyFieldEntityLabel  = "entity_label"
	legacyFieldTotal        = "total"
	legacyFieldBlocked      = "blocked"
	legacyFieldCached       = "cached"
	legacyFieldForwarded    = "forwarded"
	legacyFieldFiltered     = "filtered"
	legacyFieldLocal        = "local"
	legacyFieldOther        = "other"
	legacyFieldDropped      = "dropped"
	legacyFieldErrors       = "errors"
	legacyFieldTotalLatency = "total_latency_ms"
)

// legacyQueryLogFields restores the stored type of every query_logs column so
// sfsDb can decode the records.
//
//nolint:gochecknoglobals // immutable schema description
var legacyQueryLogFields = map[string]any{
	legacyFieldID:           0,
	legacyFieldTimestamp:    int64(0),
	legacyFieldClientIP:     "",
	legacyFieldClientName:   "",
	legacyFieldDomain:       "",
	legacyFieldQueryType:    "",
	legacyFieldResponseType: "",
	legacyFieldReason:       "",
	legacyFieldDurationMs:   uint16(0),
}

// legacyMetricFields restores the stored type of every metrics column.
//
//nolint:gochecknoglobals // immutable schema description
var legacyMetricFields = map[string]any{
	legacyFieldID:           0,
	"metric_id":             "",
	legacyFieldDimension:    "",
	legacyFieldGranularity:  "",
	legacyFieldBucketStart:  int64(0),
	legacyFieldEntity:       "",
	legacyFieldEntityLabel:  "",
	legacyFieldTotal:        uint64(0),
	legacyFieldBlocked:      uint64(0),
	legacyFieldCached:       uint64(0),
	legacyFieldForwarded:    uint64(0),
	legacyFieldFiltered:     uint64(0),
	legacyFieldLocal:        uint64(0),
	legacyFieldOther:        uint64(0),
	legacyFieldDropped:      uint64(0),
	legacyFieldErrors:       uint64(0),
	legacyFieldTotalLatency: uint64(0),
	"updated_at":            int64(0),
}

// legacyStore holds the read-only handles to the old sfsDb tables.
type legacyStore struct {
	queryLogs *engine.Table
	metrics   *engine.Table
}

// openLegacy opens the old database and restores the table schemas needed to
// decode the records.
func openLegacy(dir string) (*legacyStore, error) {
	if _, err := sfsstorage.GetDBManager().OpenDB(dir); err != nil {
		return nil, fmt.Errorf("can't open the legacy sfsDb database %s: %w", dir, err)
	}

	queryLogs, err := openLegacyTable(legacyTableQueryLogs, legacyQueryLogFields)
	if err != nil {
		return nil, err
	}

	metrics, err := openLegacyTable(legacyTableMetrics, legacyMetricFields)
	if err != nil {
		return nil, err
	}

	return &legacyStore{queryLogs: queryLogs, metrics: metrics}, nil
}

// Close releases the old database.
func (s *legacyStore) Close() {
	_ = sfsstorage.GetDBManager().CloseDB()
}

// openLegacyTable opens one table and restores its primary key so it can be
// scanned in key order.
func openLegacyTable(name string, fields map[string]any) (*engine.Table, error) {
	table, err := engine.NewTable(name)
	if err != nil {
		return nil, fmt.Errorf("can't open the legacy table %s: %w", name, err)
	}

	if err := table.SetFields(fields); err != nil {
		return nil, fmt.Errorf("can't set the fields of the legacy table %s: %w", name, err)
	}

	if table.GetIndexByName("primary") == nil {
		if err := table.CreatePrimaryKey(legacyFieldID); err != nil {
			return nil, fmt.Errorf("can't set the primary key of the legacy table %s: %w", name, err)
		}
	}

	// sfsDb caches index lookups globally by field name; clearing it makes the
	// scans use this table's own primary key.
	engine.ClearIndexMatchCache()

	return table, nil
}

// queryLogEntryFromRecord converts one old query_logs row.
func queryLogEntryFromRecord(rec *record.Record) *storage.QueryLogEntry {
	row := *rec

	return &storage.QueryLogEntry{
		Timestamp:    asInt64(row[legacyFieldTimestamp]),
		ClientIP:     asString(row[legacyFieldClientIP]),
		ClientName:   asString(row[legacyFieldClientName]),
		Domain:       asString(row[legacyFieldDomain]),
		QueryType:    asString(row[legacyFieldQueryType]),
		ResponseType: asString(row[legacyFieldResponseType]),
		Reason:       asString(row[legacyFieldReason]),
		DurationMs:   asUint16(row[legacyFieldDurationMs]),
	}
}

// metricInputFromRecord converts one old metrics row into an absolute delta.
func metricInputFromRecord(rec *record.Record) storage.MetricInput {
	row := *rec

	return storage.MetricInput{
		Dimension:   asString(row[legacyFieldDimension]),
		Granularity: asString(row[legacyFieldGranularity]),
		BucketStart: asInt64(row[legacyFieldBucketStart]),
		Entity:      asString(row[legacyFieldEntity]),
		EntityLabel: asString(row[legacyFieldEntityLabel]),
		Delta: storage.MetricsDelta{
			Total:          asUint64(row[legacyFieldTotal]),
			Blocked:        asUint64(row[legacyFieldBlocked]),
			Cached:         asUint64(row[legacyFieldCached]),
			Forwarded:      asUint64(row[legacyFieldForwarded]),
			Filtered:       asUint64(row[legacyFieldFiltered]),
			Local:          asUint64(row[legacyFieldLocal]),
			Other:          asUint64(row[legacyFieldOther]),
			Dropped:        asUint64(row[legacyFieldDropped]),
			Errors:         asUint64(row[legacyFieldErrors]),
			TotalLatencyMs: asUint64(row[legacyFieldTotalLatency]),
		},
	}
}

func asString(value any) string {
	s, _ := value.(string)

	return s
}

func asInt64(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case uint64:
		return int64(v) //nolint:gosec // legacy identifiers and buckets are small
	default:
		return 0
	}
}

func asUint64(value any) uint64 {
	switch v := value.(type) {
	case uint64:
		return v
	case int64:
		if v < 0 {
			return 0
		}

		return uint64(v)
	case int:
		if v < 0 {
			return 0
		}

		return uint64(v)
	default:
		return 0
	}
}

func asUint16(value any) uint16 {
	switch v := value.(type) {
	case uint16:
		return v
	case uint64:
		return uint16(v) //nolint:gosec // stored as uint16
	case int64:
		return uint16(v) //nolint:gosec // stored as uint16
	default:
		return 0
	}
}
