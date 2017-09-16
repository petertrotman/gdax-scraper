package main

import (
	"database/sql"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type appEnv struct {
	DB *sql.DB
}

func initApp(dataSourceName string) (*appEnv, error) {
	var app appEnv
	if err := app.initDB(dataSourceName); err != nil {
		return nil, err
	}
	return &app, nil
}

func (a *appEnv) initDB(dataSourceName string) error {
	db, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return errors.Wrap(err, "could not open the database")
	}
	if err = db.Ping(); err != nil {
		return errors.Wrap(err, "could not ping the database")
	}
	a.DB = db
	return nil
}
