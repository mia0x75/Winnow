// Command migratemetrics copies a denyzer sfsDb database (the two tables
// query_logs and metrics) into a fresh SQLite database. It is a one-shot
// operator tool: stop denyzer, point it at the old sfsDb directory and at the
// new SQLite file, run it, then configure queryLog.target for the new file.
//
// The old directory is only read; the target is written to a temporary file and
// renamed into place once everything (including the window rollups) is done.
//
//	go run ./tools/migratemetrics -from /var/lib/denyzer/old-sfsdb -to /var/lib/denyzer/denyzer.db
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/liaoran123/sfsDb/record"
	"github.com/mia0x75/denyzer/storage"
)

const (
	// batchSize is the number of rows written per transaction.
	batchSize = 5000

	// dirPerm is the mode of the directories the tool creates.
	dirPerm = 0o750

	// progressEvery logs a progress line every this many rows.
	progressEvery = 100_000
)

//nolint:gochecknoglobals // flag parsing in main is the idiomatic one-shot tool shape
var (
	flagFrom = flag.String("from", "", "legacy sfsDb directory to read")
	flagTo   = flag.String("to", "", "SQLite file to create (must not exist)")
)

func main() {
	flag.Parse()

	if err := run(*flagFrom, *flagTo); err != nil {
		fmt.Fprintf(os.Stderr, "migration failed: %v\n", err)

		os.Exit(1)
	}
}

// run performs the whole migration.
func run(from, to string) error {
	if from == "" || to == "" {
		return errors.New("both -from and -to are required")
	}

	if err := checkTarget(to); err != nil {
		return err
	}

	started := time.Now()

	legacy, err := openLegacy(from)
	if err != nil {
		return err
	}

	defer legacy.Close()

	tmp, err := openTarget(to)
	if err != nil {
		return err
	}

	cleanup := true
	defer func() {
		if cleanup {
			_ = storage.CloseDB()
			_ = os.Remove(tmp)
		}
	}()

	logs, metrics, err := importAll(legacy)
	if err != nil {
		return err
	}

	if logs == 0 && metrics == 0 {
		return errors.New("no query_logs or metrics rows found: is -from the two-table sfsDb directory?")
	}

	if err := storage.CloseDB(); err != nil {
		return fmt.Errorf("can't close the target database: %w", err)
	}

	if err := os.Rename(tmp, to); err != nil {
		return fmt.Errorf("can't move %s to %s: %w", tmp, to, err)
	}

	cleanup = false

	fmt.Printf("query_logs: %d rows\nmetrics:   %d rows\nelapsed:   %s\ntarget:    %s\n",
		logs, metrics, time.Since(started).Round(time.Millisecond), to)

	return nil
}

// checkTarget refuses to overwrite an existing database or a directory.
func checkTarget(to string) error {
	info, err := os.Stat(to)
	switch {
	case err == nil && info.IsDir():
		return fmt.Errorf("target %s is a directory; pass the database file path", to)
	case err == nil:
		return fmt.Errorf("target %s already exists; move it away first", to)
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("can't stat target %s: %w", to, err)
	}

	if err := os.MkdirAll(filepath.Dir(to), dirPerm); err != nil {
		return fmt.Errorf("can't create target directory: %w", err)
	}

	return nil
}

// openTarget creates the temporary target database next to the final path and
// returns that temporary path.
func openTarget(to string) (string, error) {
	tmp := to + ".tmp"

	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("can't remove stale temporary file %s: %w", tmp, err)
	}

	if err := storage.InitDB(tmp); err != nil {
		return "", err
	}

	return tmp, nil
}

// importAll copies both tables and rebuilds the window rollups from the day
// buckets, so the new database is ready for the dashboard.
func importAll(legacy *legacyStore) (int, int, error) {
	logs, err := importQueryLogs(legacy)
	if err != nil {
		return 0, 0, err
	}

	metrics, err := importMetrics(legacy)
	if err != nil {
		return 0, 0, err
	}

	if err := storage.MaintainWindowRollups(time.Now()); err != nil {
		return 0, 0, fmt.Errorf("can't build the window rollups: %w", err)
	}

	return logs, metrics, nil
}

// importQueryLogs streams every old query log row into the target database.
func importQueryLogs(legacy *legacyStore) (int, error) {
	total := 0
	pending := make([]*storage.QueryLogEntry, 0, batchSize)

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}

		if err := storage.BatchInsertQueryLogs(pending); err != nil {
			return err
		}

		total += len(pending)
		pending = pending[:0]

		if total%progressEvery < batchSize {
			fmt.Printf("query_logs: %d rows\n", total)
		}

		return nil
	}

	var readErr error

	iter := legacy.queryLogs.ForData()
	defer iter.Release()

	iter.ExportRecord(func(rec *record.Record) bool {
		pending = append(pending, queryLogEntryFromRecord(rec))

		if len(pending) < batchSize {
			return true
		}

		if err := flush(); err != nil {
			readErr = err

			return false
		}

		return true
	}, true)

	if readErr != nil {
		return total, readErr
	}

	if err := flush(); err != nil {
		return total, err
	}

	return total, nil
}

// flushMetricBatch commits one batch of metric inputs and reports how many rows
// it wrote.
func flushMetricBatch(pending []storage.MetricInput) (int, error) {
	if len(pending) == 0 {
		return 0, nil
	}

	batch, err := storage.NewBatch()
	if err != nil {
		return 0, err
	}

	if err := storage.UpsertMetrics(batch, pending); err != nil {
		storage.RollbackBatch(batch)

		return 0, err
	}

	if err := storage.CommitBatch(batch); err != nil {
		return 0, err
	}

	return len(pending), nil
}

// importMetrics streams every old metric row into the target database. The old
// rows are already absolute bucket totals, so they become delta inputs to a
// fresh database.
func importMetrics(legacy *legacyStore) (int, error) {
	total := 0
	pending := make([]storage.MetricInput, 0, batchSize)

	flush := func() error {
		committed, err := flushMetricBatch(pending)
		if err != nil {
			return err
		}

		if committed == 0 {
			return nil
		}

		total += committed
		pending = pending[:0]

		if total%progressEvery < batchSize {
			fmt.Printf("metrics:    %d rows\n", total)
		}

		return nil
	}

	var readErr error

	iter := legacy.metrics.ForData()
	defer iter.Release()

	iter.ExportRecord(func(rec *record.Record) bool {
		pending = append(pending, metricInputFromRecord(rec))

		if len(pending) < batchSize {
			return true
		}

		if err := flush(); err != nil {
			readErr = err

			return false
		}

		return true
	}, true)

	if readErr != nil {
		return total, readErr
	}

	if err := flush(); err != nil {
		return total, err
	}

	return total, nil
}
