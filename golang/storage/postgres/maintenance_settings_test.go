package postgres

import (
	"context"
	"strings"
	"testing"
)

func TestInspectTableSettingsFailsClosedWithoutConfiguredPool(t *testing.T) {
	if _, err := (MaintenanceRepository{}).InspectTableSettings(context.Background()); err == nil {
		t.Fatal("settings inspection succeeded without a PostgreSQL pool")
	}
	if _, err := (MaintenanceRepository{}).InspectTableSettings(nil); err == nil {
		t.Fatal("settings inspection succeeded with a nil context")
	}
}

func TestMaintenanceSettingsQueryIsReadOnlyAndNamespaceBound(t *testing.T) {
	query := maintenanceSettingsQuery()
	for _, forbidden := range []string{"DELETE", "UPDATE", "INSERT", "TRUNCATE", "ALTER", "DROP"} {
		if strings.Contains(strings.ToUpper(query), forbidden) {
			t.Fatalf("maintenance settings query contains mutating verb %q: %s", forbidden, query)
		}
	}
	for _, required := range []string{
		"pg_class", "pg_namespace", "pg_stat_user_tables", "n.nspname = $1", "c.relname LIKE $2", "c.reloptions", "s.n_dead_tup",
	} {
		if !strings.Contains(query, required) {
			t.Fatalf("maintenance settings query omits %q: %s", required, query)
		}
	}
}

func TestMaintenanceSettingsExpectedRelationsAreExplicit(t *testing.T) {
	if len(expectedMaintenanceSettingsRelations) != len(maintenanceSettingsResources) {
		t.Fatalf("expected relation fence has %d entries for %d resources", len(expectedMaintenanceSettingsRelations), len(maintenanceSettingsResources))
	}
	for _, relation := range expectedMaintenanceSettingsRelations {
		if _, ok := maintenanceSettingsResources[relation]; !ok {
			t.Fatalf("expected relation %q is not represented by a logical resource", relation)
		}
	}
}

func TestDecodeMaintenanceReloptionsReportsActualValuesWithoutDefaults(t *testing.T) {
	var settings TableMaintenanceSettings
	if err := decodeMaintenanceReloptions([]string{
		"fillfactor=80",
		"autovacuum_vacuum_threshold=1000",
		"autovacuum_vacuum_scale_factor=0.05",
		"autovacuum_analyze_threshold=500",
		"autovacuum_analyze_scale_factor=0.02",
		"autovacuum_enabled=false",
		"toast.autovacuum_enabled=true",
	}, &settings); err != nil {
		t.Fatal(err)
	}
	if settings.Fillfactor == nil || *settings.Fillfactor != 80 {
		t.Fatalf("fillfactor=%v, want actual reloption 80", settings.Fillfactor)
	}
	if settings.AutovacuumVacuumThreshold == nil || *settings.AutovacuumVacuumThreshold != 1000 {
		t.Fatalf("vacuum threshold=%v, want actual reloption 1000", settings.AutovacuumVacuumThreshold)
	}
	if settings.AutovacuumVacuumScaleFactor == nil || *settings.AutovacuumVacuumScaleFactor != 0.05 {
		t.Fatalf("vacuum scale factor=%v, want actual reloption 0.05", settings.AutovacuumVacuumScaleFactor)
	}
	if settings.AutovacuumAnalyzeThreshold == nil || *settings.AutovacuumAnalyzeThreshold != 500 {
		t.Fatalf("analyze threshold=%v, want actual reloption 500", settings.AutovacuumAnalyzeThreshold)
	}
	if settings.AutovacuumAnalyzeScaleFactor == nil || *settings.AutovacuumAnalyzeScaleFactor != 0.02 {
		t.Fatalf("analyze scale factor=%v, want actual reloption 0.02", settings.AutovacuumAnalyzeScaleFactor)
	}
	if settings.AutovacuumEnabled == nil || *settings.AutovacuumEnabled {
		t.Fatalf("autovacuum enabled=%v, want explicit false reloption", settings.AutovacuumEnabled)
	}
	if settings.ToastAutovacuumEnabled == nil || !*settings.ToastAutovacuumEnabled {
		t.Fatalf("toast autovacuum enabled=%v, want explicit true reloption", settings.ToastAutovacuumEnabled)
	}

	var defaults TableMaintenanceSettings
	if err := decodeMaintenanceReloptions(nil, &defaults); err != nil {
		t.Fatal(err)
	}
	if defaults.Fillfactor != nil || defaults.AutovacuumVacuumScaleFactor != nil || defaults.AutovacuumEnabled != nil {
		t.Fatalf("missing reloptions were converted into guessed defaults: %+v", defaults)
	}
}

func TestDecodeMaintenanceReloptionsFailsClosed(t *testing.T) {
	for _, option := range []string{"fillfactor", "fillfactor=9", "fillfactor=101", "autovacuum_vacuum_threshold=-1", "autovacuum_vacuum_scale_factor=-0.1", "autovacuum_analyze_threshold=nope", "autovacuum_analyze_scale_factor=nope", "autovacuum_enabled=maybe", "toast.autovacuum_enabled=maybe"} {
		t.Run(option, func(t *testing.T) {
			if err := decodeMaintenanceReloptions([]string{option}, &TableMaintenanceSettings{}); err == nil {
				t.Fatalf("invalid reloption %q was accepted", option)
			}
		})
	}
	if err := decodeMaintenanceReloptions([]string{"fillfactor=80", "fillfactor=90"}, &TableMaintenanceSettings{}); err == nil {
		t.Fatal("duplicate reloption was accepted")
	}
}
