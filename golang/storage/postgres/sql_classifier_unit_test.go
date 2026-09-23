package postgres

import "testing"

func TestClassifySQLFailsClosedForUnknownAndComments(t *testing.T) {
	tests := []struct {
		name       string
		statement  string
		wantKind   SQLStatementKind
		wantBudget bool
		wantRead   bool
	}{
		{name: "insert relation", statement: `INSERT INTO "private"."budget_buckets" (window_id) VALUES ($1)`, wantKind: SQLStatementInsert, wantBudget: true},
		{name: "update relation", statement: `UPDATE "private"."operation_budget_reservations" SET state=$1`, wantKind: SQLStatementUpdate, wantBudget: true},
		{name: "select relation", statement: `SELECT * FROM "private"."budget_windows"`, wantKind: SQLStatementSelect, wantBudget: true, wantRead: true},
		{name: "insert selects budget relation", statement: `INSERT INTO "private"."budget_buckets" SELECT * FROM "private"."budget_windows"`, wantKind: SQLStatementInsert, wantBudget: true, wantRead: true},
		{name: "update reads from budget relation", statement: `UPDATE "private"."budget_buckets" SET state=$1 FROM "private"."budget_windows"`, wantKind: SQLStatementUpdate, wantBudget: true, wantRead: true},
		{name: "write selects unrelated relation", statement: `INSERT INTO "private"."budget_buckets" SELECT * FROM fixture_rows`, wantKind: SQLStatementInsert, wantBudget: true},
		{name: "literal is not relation", statement: `SELECT 'budget_buckets'`, wantKind: SQLStatementSelect},
		{name: "comment is not relation", statement: "-- SELECT * FROM budget_buckets\nINSERT INTO operations VALUES ($1)", wantKind: SQLStatementInsert},
		{name: "unknown CTE", statement: `WITH rows AS (SELECT 1) INSERT INTO budget_buckets SELECT * FROM rows`, wantKind: SQLStatementUnknown, wantBudget: true, wantRead: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ClassifySQL(test.statement)
			if got.Kind != test.wantKind || got.BudgetTable != test.wantBudget || got.BudgetRead != test.wantRead {
				t.Fatalf("ClassifySQL(%q) = %#v, want kind=%d budget=%v read=%v", test.statement, got, test.wantKind, test.wantBudget, test.wantRead)
			}
		})
	}
}

func TestClassifySQLMatchesRenderedPrefixedRelations(t *testing.T) {
	namespace, err := NewNamespace("worker_db", "worker_state", "tenant_")
	if err != nil {
		t.Fatal(err)
	}
	buckets, err := namespace.Render("budget_buckets")
	if err != nil {
		t.Fatal(err)
	}
	windows, err := namespace.Render("budget_windows")
	if err != nil {
		t.Fatal(err)
	}
	classified := ClassifySQLWithRelations(
		`INSERT INTO `+buckets+` SELECT * FROM `+windows,
		buckets, windows,
	)
	if !classified.BudgetTable || !classified.BudgetRead || classified.Kind != SQLStatementInsert {
		t.Fatalf("prefixed budget read classified as %#v", classified)
	}
}
