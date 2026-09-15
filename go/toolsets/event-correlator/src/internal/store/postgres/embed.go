package postgres

import (
	"embed"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

func migrationSQL() (string, error) {
	contents, err := migrationFS.ReadFile("migrations/00001_initial_schema.sql")
	if err != nil {
		return "", err
	}
	up, _, _ := strings.Cut(string(contents), "-- +goose Down")
	return strings.Replace(up, "-- +goose Up", "", 1), nil
}
