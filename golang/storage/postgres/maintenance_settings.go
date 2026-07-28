package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TableMaintenanceSettings is a read-only snapshot of PostgreSQL's table
// options and tuple estimates for one maintenance-relevant table. A nil
// option means PostgreSQL is using its server/database default; this package
// deliberately does not fill in or infer that default.
type TableMaintenanceSettings struct {
	Resource                     string
	Fillfactor                   *int
	AutovacuumVacuumThreshold    *int64
	AutovacuumVacuumScaleFactor  *float64
	AutovacuumAnalyzeThreshold   *int64
	AutovacuumAnalyzeScaleFactor *float64
	LiveTuples                   int64
	DeadTuples                   int64
	LastAutovacuum               *time.Time
	LastAutoanalyze              *time.Time
}

var maintenanceSettingsResources = map[string]string{
	"operations":             "operation",
	"budget_buckets":         "budget",
	"response_cache_entries": "cache",
	"response_cache_fills":   "cache_fill",
	"provider_route_status":  "status",
	"maintenance_outbox":     "outbox",
}

// maintenanceSettingsQuery reads only PostgreSQL catalogs/statistics. The
// namespace and prefix are values, not identifiers, so a caller cannot make
// this diagnostic query leave its worker-owned namespace.
func maintenanceSettingsQuery() string {
	return `SELECT c.relname, c.reloptions, s.n_live_tup, s.n_dead_tup,
       s.last_autovacuum, s.last_autoanalyze
FROM pg_class AS c
JOIN pg_namespace AS n ON n.oid = c.relnamespace
JOIN pg_stat_user_tables AS s ON s.relid = c.oid
WHERE n.nspname = $1
  AND c.relkind = 'r'
  AND c.relname LIKE $2
ORDER BY c.relname`
}

// InspectTableSettings returns actual reloptions and PostgreSQL statistics for
// known maintenance tables. It is intentionally read-only and fails closed on
// catalog/query/option decoding errors; an empty result is not converted into
// guessed defaults.
func (repository MaintenanceRepository) InspectTableSettings(ctx context.Context) ([]TableMaintenanceSettings, error) {
	if ctx == nil {
		return nil, errors.New("maintenance settings context is nil")
	}
	if err := repository.validate(); err != nil {
		return nil, err
	}
	rows, err := repository.Pool.Query(ctx, maintenanceSettingsQuery(), repository.Namespace.Schema, repository.Namespace.TablePrefix+"%")
	if err != nil {
		return nil, redactPostgresError(fmt.Errorf("inspect maintenance table settings: %w", err))
	}
	defer rows.Close()
	settings := make([]TableMaintenanceSettings, 0, len(maintenanceSettingsResources))
	for rows.Next() {
		var relation string
		var options []string
		var item TableMaintenanceSettings
		if err := rows.Scan(&relation, &options, &item.LiveTuples, &item.DeadTuples, &item.LastAutovacuum, &item.LastAutoanalyze); err != nil {
			return nil, fmt.Errorf("scan maintenance table settings: %w", err)
		}
		logical := strings.TrimPrefix(relation, repository.Namespace.TablePrefix)
		resource, ok := maintenanceSettingsResources[logical]
		if !ok {
			continue
		}
		item.Resource = resource
		if err := decodeMaintenanceReloptions(options, &item); err != nil {
			return nil, fmt.Errorf("decode maintenance settings for %s: %w", resource, err)
		}
		settings = append(settings, item)
	}
	if err := rows.Err(); err != nil {
		return nil, redactPostgresError(fmt.Errorf("iterate maintenance table settings: %w", err))
	}
	return settings, nil
}

func decodeMaintenanceReloptions(options []string, settings *TableMaintenanceSettings) error {
	if settings == nil {
		return errors.New("maintenance settings output is nil")
	}
	seen := make(map[string]struct{}, len(options))
	for _, option := range options {
		key, value, ok := strings.Cut(option, "=")
		if !ok || value == "" {
			return fmt.Errorf("malformed reloption %q", option)
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate reloption %q", key)
		}
		seen[key] = struct{}{}
		switch key {
		case "fillfactor":
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < 10 || parsed > 100 {
				return fmt.Errorf("fillfactor %q is outside PostgreSQL's 10..100 range", value)
			}
			settings.Fillfactor = &parsed
		case "autovacuum_vacuum_threshold":
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil || parsed < 0 {
				return fmt.Errorf("autovacuum vacuum threshold %q is invalid", value)
			}
			settings.AutovacuumVacuumThreshold = &parsed
		case "autovacuum_vacuum_scale_factor":
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil || parsed < 0 {
				return fmt.Errorf("autovacuum vacuum scale factor %q is invalid", value)
			}
			settings.AutovacuumVacuumScaleFactor = &parsed
		case "autovacuum_analyze_threshold":
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil || parsed < 0 {
				return fmt.Errorf("autovacuum analyze threshold %q is invalid", value)
			}
			settings.AutovacuumAnalyzeThreshold = &parsed
		case "autovacuum_analyze_scale_factor":
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil || parsed < 0 {
				return fmt.Errorf("autovacuum analyze scale factor %q is invalid", value)
			}
			settings.AutovacuumAnalyzeScaleFactor = &parsed
		}
	}
	return nil
}
