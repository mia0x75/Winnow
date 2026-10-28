package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/liaoran123/sfsDb/engine"
	sfsstorage "github.com/liaoran123/sfsDb/storage"
	"github.com/mia0x75/denyzer/storage"
)

// TestMigrate seeds a small legacy sfsDb database, migrates it and verifies
// that the rows, the all-time counters and the window rollups survive.
func TestMigrate(t *testing.T) {
	dir := t.TempDir()
	legacyDir := filepath.Join(dir, "legacy")
	target := filepath.Join(dir, "denyzer.db")

	seedLegacy(t, legacyDir)

	// The legacy store is a process-wide singleton; make the next open start
	// from a clean table-ID state, exactly like a fresh process would.
	engine.TableIDManager = nil

	if err := run(legacyDir, target); err != nil {
		t.Fatalf("run: %v", err)
	}

	if err := storage.InitDB(target); err != nil {
		t.Fatalf("InitDB: %v", err)
	}

	t.Cleanup(func() { _ = storage.CloseDB() })

	total, err := storage.GetQueryLogsTotal()
	if err != nil {
		t.Fatalf("GetQueryLogsTotal: %v", err)
	}

	if total != 3 {
		t.Fatalf("all-time total = %d, want 3", total)
	}

	page, err := storage.GetQueryLogsPage("", 10, "", "", "", "")
	if err != nil {
		t.Fatalf("GetQueryLogsPage: %v", err)
	}

	if len(page.Entries) != 3 {
		t.Fatalf("query log rows = %d, want 3", len(page.Entries))
	}

	if page.Entries[2].Domain != "oldest.test" || page.Entries[0].Domain != "newest.test" {
		t.Fatalf("query log order = %q..%q", page.Entries[2].Domain, page.Entries[0].Domain)
	}

	domains, err := storage.GetTopEntities(storage.DimensionDomain, storage.GranularityWindow7d, 0, 0)
	if err != nil {
		t.Fatalf("GetTopEntities: %v", err)
	}

	byEntity := make(map[string]uint64, len(domains))

	for _, entity := range domains {
		byEntity[entity.Entity] = entity.Total
	}

	// The rollup is rebuilt from the day buckets, so it must contain the day
	// row and not the stale rollup row that was also present in the old store.
	if byEntity["rolled.test"] != 4 {
		t.Fatalf("rebuilt rollup total for rolled.test = %d, want 4 (%v)", byEntity["rolled.test"], byEntity)
	}

	if _, ok := byEntity["stale.test"]; ok {
		t.Fatalf("stale rollup row was copied instead of rebuilt: %v", byEntity)
	}
}

// seedLegacy creates the old sfsDb layout with three query log rows, an
// all-time counter, a day bucket and a stale window rollup.
func seedLegacy(t *testing.T, dir string) {
	t.Helper()

	if _, err := sfsstorage.GetDBManager().OpenDB(dir); err != nil {
		t.Fatalf("open legacy: %v", err)
	}

	queryLogs := openLegacyTableForSeed(t, legacyTableQueryLogs, legacyQueryLogFields)
	for _, row := range []map[string]any{
		{legacyFieldTimestamp: int64(1_000), legacyFieldClientIP: "10.0.0.1", legacyFieldClientName: "", legacyFieldDomain: "oldest.test", legacyFieldQueryType: "A", legacyFieldResponseType: "RESOLVED", legacyFieldReason: "", legacyFieldDurationMs: uint16(1)},
		{legacyFieldTimestamp: int64(2_000), legacyFieldClientIP: "10.0.0.2", legacyFieldClientName: "laptop", legacyFieldDomain: "middle.test", legacyFieldQueryType: "A", legacyFieldResponseType: "BLOCKED", legacyFieldReason: "ads", legacyFieldDurationMs: uint16(2)},
		{legacyFieldTimestamp: int64(3_000), legacyFieldClientIP: "10.0.0.3", legacyFieldClientName: "", legacyFieldDomain: "newest.test", legacyFieldQueryType: "AAAA", legacyFieldResponseType: "CACHED", legacyFieldReason: "", legacyFieldDurationMs: uint16(3)},
	} {
		insertLegacyRow(t, queryLogs, row)
	}

	metrics := openLegacyTableForSeed(t, legacyTableMetrics, legacyMetricFields)
	for _, row := range []map[string]any{
		{
			legacyFieldDimension: storage.DimensionGlobal, legacyFieldGranularity: storage.GranularityAllTime,
			legacyFieldBucketStart: int64(0), legacyFieldEntity: "", legacyFieldTotal: uint64(3),
		},
		{
			legacyFieldDimension: storage.DimensionDomain, legacyFieldGranularity: storage.GranularityDay,
			legacyFieldBucketStart: storage.DayBucket(time.Now()), legacyFieldEntity: "rolled.test",
			legacyFieldEntityLabel: "rolled.test", legacyFieldTotal: uint64(4),
		},
		{
			legacyFieldDimension: storage.DimensionDomain, legacyFieldGranularity: storage.GranularityWindow7d,
			legacyFieldBucketStart: int64(0), legacyFieldEntity: "stale.test",
			legacyFieldEntityLabel: "stale.test", legacyFieldTotal: uint64(9),
		},
	} {
		insertLegacyRow(t, metrics, row)
	}

	if err := sfsstorage.GetDBManager().CloseDB(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}
}

func openLegacyTableForSeed(t *testing.T, name string, fields map[string]any) *engine.Table {
	t.Helper()

	table, err := engine.NewTable(name)
	if err != nil {
		t.Fatalf("NewTable %s: %v", name, err)
	}

	if err := table.SetFields(fields); err != nil {
		t.Fatalf("SetFields %s: %v", name, err)
	}

	if err := table.CreatePrimaryKey(legacyFieldID); err != nil {
		t.Fatalf("CreatePrimaryKey %s: %v", name, err)
	}

	engine.ClearIndexMatchCache()
	table.InitAuto()
	engine.ClearIndexMatchCache()

	return table
}

func insertLegacyRow(t *testing.T, table *engine.Table, row map[string]any) {
	t.Helper()

	row[legacyFieldID] = nil

	if _, err := table.Insert(&row); err != nil {
		t.Fatalf("Insert legacy row: %v", err)
	}
}
