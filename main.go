package main

import (
	"fmt"
	"log"
	"os"

	"github.com/pkg/errors"
)

func main() {
	dataSourceName, err := getDataSourceName()
	if err != nil {
		log.Fatal(errors.Wrap(err, "could not get data source name"))
	}

	app, err := initApp(dataSourceName)
	if err != nil {
		log.Fatal(errors.Wrap(err, "could not initialize app"))
	}

	_ = app

	// ch := gdax.Snapshots([]string{"BTC-USD", "BTC-EUR"})
	// fmt.Println(<-ch)

	// snapshot, err := gdax.Snapshot("BTC-USD")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println(snapshot)

	// channels := []gdax.Channel{
	// 	gdax.Channel{"full", []string{"BTC-USD"}},
	// }
	// data, errs, err := gdax.Subscribe(channels)
	// if err != nil {
	// 	log.Fatal("could not subscribe to gdax: ", err)
	// }

	// for {
	// 	select {
	// 	case msg := <-data:
	// 		fmt.Println(msg)
	// 	case err = <-errs:
	// 		log.Println(err)
	// 	}
	// }
}

func getDataSourceName() (string, error) {
	pgUser := os.Getenv("POSTGRES_USER")
	if pgUser == "" {
		return "", fmt.Errorf("no POSTGRES_USER")
	}
	pgPassword := os.Getenv("POSTGRES_PASSWORD")
	if pgPassword == "" {
		return "", fmt.Errorf("no POSTGRES_PASSWORD")
	}
	pgHost := os.Getenv("POSTGRES_HOST")
	if pgHost == "" {
		return "", fmt.Errorf("no POSTGRES_HOST")
	}
	pgDB := os.Getenv("POSTGRES_DB")
	if pgDB == "" {
		return "", fmt.Errorf("no POSTGRES_DB")
	}
	env := os.Getenv("ENV")

	if env == "development" {
		return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", pgUser, pgPassword, pgHost, pgDB), nil
	}

	return fmt.Sprintf("postgres://%s:%s@%s/%s", pgUser, pgPassword, pgHost, pgDB), nil
}
