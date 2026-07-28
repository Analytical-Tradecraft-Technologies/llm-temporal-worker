package postgres

import "testing"

func TestInspectTableSettingsReadsConfiguredPostgresValues(t *testing.T) {
	repository, ctx, cleanup := maintenanceIntegrationRepository(t)
	defer cleanup()

	settings, err := repository.InspectTableSettings(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(settings) == 0 {
		t.Fatal("settings inspection returned no known maintenance tables")
	}
	var foundOperation bool
	for _, item := range settings {
		if item.Resource != "operation" {
			continue
		}
		foundOperation = true
		if item.Fillfactor == nil || *item.Fillfactor != 80 {
			t.Fatalf("operation fillfactor=%v, want configured schema value 80", item.Fillfactor)
		}
		if item.LiveTuples < 0 || item.DeadTuples < 0 {
			t.Fatalf("negative PostgreSQL tuple estimates: %+v", item)
		}
	}
	if !foundOperation {
		t.Fatal("settings inspection omitted the operations table")
	}
}
