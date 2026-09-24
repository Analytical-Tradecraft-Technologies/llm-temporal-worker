package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mfow/llm-temporal-worker/golang/storage/postgres"
)

// Schema administration is deliberately separate from worker startup. The
// binary's embedded migrations and installer are the only schema authority.
func executeSchemaCommand(ctx context.Context, args []string, options CommandOptions) int {
	if len(args) == 0 || (args[0] != "render" && args[0] != "install" && args[0] != "verify") {
		writeCommandError(options.ErrOut, errors.New("usage: schema <render|install|verify> --database NAME --schema NAME --table-prefix PREFIX"))
		return 2
	}
	action := args[0]
	flags := flag.NewFlagSet("schema "+action, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	database := flags.String("database", "", "expected PostgreSQL database")
	schema := flags.String("schema", "public", "immutable PostgreSQL schema")
	prefix := flags.String("table-prefix", "llmtw_", "immutable worker object prefix; may be empty for a verified existing namespace")
	address := flags.String("address", "", "PostgreSQL host:port")
	serverName := flags.String("server-name", "", "verified PostgreSQL TLS server name")
	caFile := flags.String("ca-file", "", "PostgreSQL TLS CA PEM file")
	usernameFile := flags.String("username-file", "", "PostgreSQL username file")
	passwordFile := flags.String("password-file", "", "PostgreSQL password file")
	allowUpgrade := flags.Bool("allow-upgrade", false, "explicitly allow checksum-verified ordered upgrades in the existing namespace")
	timeout := flags.Duration("timeout", 2*time.Minute, "bounded administration deadline (at most 10m)")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 || *timeout <= 0 || *timeout > 10*time.Minute || (*allowUpgrade && action != "install") {
		writeCommandError(options.ErrOut, errors.New("invalid schema command arguments"))
		return 2
	}
	namespace, err := postgres.NewNamespace(*database, *schema, *prefix)
	if err != nil {
		writeCommandError(options.ErrOut, err)
		return 2
	}
	if action == "render" {
		sql, err := postgres.RenderMigration(namespace)
		if err == nil {
			_, err = io.WriteString(options.Out, "-- Review only: use schema install to validate history, record exact digests and reconcile grants.\n"+sql)
		}
		if err != nil {
			writeCommandError(options.ErrOut, err)
			return 1
		}
		return 0
	}
	if *address == "" || *serverName == "" || *caFile == "" || *usernameFile == "" || *passwordFile == "" {
		writeCommandError(options.ErrOut, errors.New("schema administration requires address, verified TLS and database login files"))
		return 2
	}
	username, err := readSchemaLoginFile(*usernameFile)
	if err != nil {
		writeCommandError(options.ErrOut, err)
		return 2
	}
	password, err := readSchemaLoginFile(*passwordFile)
	if err != nil {
		writeCommandError(options.ErrOut, err)
		return 2
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	pool, err := postgres.NewPool(ctx, postgres.PoolOptions{
		Namespace: namespace, Addresses: []string{*address}, Username: username, Password: password,
		TLS:            postgres.TLSOptions{Enabled: true, ServerName: *serverName, CAFile: *caFile},
		MaxConnections: 2, MinConnections: 0, DialTimeout: 10 * time.Second,
		StatementTimeout: *timeout, LockTimeout: 10 * time.Second, IdleTxTimeout: *timeout,
		ApplicationName: "llmtw-schema-" + action,
	})
	if err != nil {
		// A server error may include submitted values: do not surface raw
		// connection diagnostics from this privileged, noninteractive command.
		writeCommandError(options.ErrOut, errors.New("schema PostgreSQL connection failed; check the endpoint, TLS and login files"))
		return 1
	}
	defer pool.Close()
	if action == "install" {
		if err = preflightSchemaInstall(ctx, pool, namespace, *allowUpgrade); err == nil {
			err = postgres.Install(ctx, pool, namespace)
		}
	}
	if err == nil {
		err = postgres.Verify(ctx, pool, namespace)
	}
	if err != nil {
		writeCommandError(options.ErrOut, err)
		if action == "install" {
			_, _ = io.WriteString(options.ErrOut, "No legacy adoption or namespace move is performed. Stop writers, back up the database, and compare schema render with the original release. Restore verified markers/layout in that same namespace or use a reviewed data-preserving migration; never delete markers or change the prefix to bypass this failure.\n")
		}
		return 1
	}
	_, _ = fmt.Fprintf(options.Out, "%s %s %s\n", action, postgres.ContractVersion, namespace.String())
	return 0
}

func readSchemaLoginFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", errors.New("cannot read database login file")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 65537))
	if err != nil || len(data) == 0 || len(data) > 65536 || strings.ContainsAny(string(data), "\x00\r\n") {
		return "", errors.New("database login file must contain one nonempty value without a trailing newline")
	}
	return string(data), nil
}

func preflightSchemaInstall(ctx context.Context, pool *pgxpool.Pool, namespace postgres.Namespace, allowUpgrade bool) error {
	// Do not silently create an empty prefixed namespace next to old chart
	// tables. Derive candidate names from the embedded migration, not a second
	// catalog. Other contract tables in this database require explicit operator
	// reconciliation, even if the requested target already exists.
	unprefixed := namespace
	unprefixed.TablePrefix = ""
	sql, err := postgres.RenderMigration(unprefixed)
	if err != nil {
		return err
	}
	var legacyTables []string
	for _, line := range strings.Split(sql, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == "CREATE" && fields[1] == "TABLE" {
			legacyTables = append(legacyTables, strings.TrimSuffix(strings.TrimPrefix(fields[2], namespace.Schema+"."), "("))
		}
	}
	var legacy bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (
SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND EXISTS (
    SELECT 1 FROM unnest($3::text[]) AS worker_table(logical_name)
    WHERE (c.relname = logical_name OR right(c.relname, length(logical_name) + 1) = '_' || logical_name)
      AND NOT (n.nspname = $1 AND c.relname = $2 || logical_name)
  )
)`, namespace.Schema, namespace.TablePrefix, legacyTables).Scan(&legacy); err != nil {
		return errors.New("cannot inspect existing worker namespace; refusing schema installation")
	}
	if legacy {
		return errors.New("legacy or alternate worker namespace detected; refusing to start empty alongside existing state")
	}
	relation, err := namespace.Render("schema_contract")
	if err != nil {
		return err
	}
	var existing *string
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1)::text", relation).Scan(&existing); err != nil {
		return errors.New("cannot inspect schema history; refusing schema installation")
	}
	if existing != nil && !allowUpgrade {
		if err := postgres.Verify(ctx, pool, namespace); err != nil {
			return errors.New("existing schema is not current and verified; inspect it, then explicitly use --allow-upgrade for known ordered migrations")
		}
	}
	return nil
}
