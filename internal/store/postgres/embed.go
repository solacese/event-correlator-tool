package postgres

import (
	"embed"
	"io/fs"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

func migrations() fs.FS {
	sub, err := fs.Sub(migrationFS, "migrations")
	if err != nil {
		panic("embedded migration FS: " + err.Error())
	}
	return sub
}
