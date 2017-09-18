package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/petertrotman/gdax-scraper/gdax"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "gdax-scraper"
	app.Usage = "gather and store real time orders data from GDAX"
	cli.VersionFlag = cli.BoolFlag{
		Name: "version",
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "database, d",
			Usage:  "connect to database at `DATABASE_URI`",
			EnvVar: "DATABASE_URI",
		},
		cli.StringSliceFlag{
			Name:  "products, p",
			Usage: "list of products to follow",
			Value: &cli.StringSlice{"all"},
		},
		cli.IntFlag{
			Name:  "snapshots-interval",
			Usage: "interval, in minutes, between each orderbook snapshot",
			Value: 60,
		},
		cli.BoolFlag{
			Name:  "batch, b",
			Usage: "enable batch inserting of messages and snapshots",
		},
		cli.BoolFlag{
			Name:  "verbose, v",
			Usage: "increased logging to stdout",
		},
	}
	app.Action = func(c *cli.Context) error {
		err := run(&runOpts{
			databaseURI:       c.String("database"),
			productIDs:        c.StringSlice("products")[1:],
			snapshotsInterval: c.Int("snapshots-interval"),
			batch:             c.Bool("batch"),
			verbose:           c.Bool("verbose"),
		})
		if err != nil {
			return cli.NewExitError(err, 1)
		}
		return nil
	}
	app.Run(os.Args)
}

type runOpts struct {
	databaseURI       string
	productIDs        []string
	snapshotsInterval int
	batch             bool
	verbose           bool
}

func run(opts *runOpts) error {
	productIDs, err := getProductIDs(opts.productIDs)
	if err != nil {
		return errors.Wrap(err, "could not get list of product ids")
	}

	db, err := sql.Open("postgres", opts.databaseURI)
	if err != nil {
		return errors.Wrap(err, "could not open the database")
	}
	if err = db.Ping(); err != nil {
		return errors.Wrap(err, "could not ping the database")
	}
	if err = prepareDB(db); err != nil {
		return errors.Wrap(err, "could not prepare the database")
	}
	stmts, err := prepareStmts(db)
	if err != nil {
		return errors.Wrap(err, "could not prepare database statements")
	}

	snapshots := gdax.GetSnapshotsEvery(productIDs, time.Duration(opts.snapshotsInterval)*time.Minute)
	messages, err := gdax.Subscribe([]gdax.Channel{{"full", productIDs}})
	if err != nil {
		return errors.Wrap(err, "could not generate websocket subscription")
	}

	var batchMessages chan *gdax.Message
	var batchMessagesResult chan insertResult
	if opts.batch {
		batchMessages = make(chan *gdax.Message)
		batchMessagesResult = batchInsertMessages(db, batchMessages)
	}

	for {
		select {

		case s := <-snapshots:
			if s.Error != nil {
				log.Println(errors.Wrap(s.Error, "error from snapshots feed"))
			} else {
				go func() {
					var err error
					if opts.batch {
						_, err = batchInsertSnapshot(db, s.Snapshot)
					} else {
						_, err = insertSnapshot(stmts, s.Snapshot)
					}
					if err != nil {
						log.Println(errors.Wrap(err, "could not insert snapshot"))
					}
				}()
				if opts.verbose {
					fmt.Println("SnapshotResult: ", s)
				}
			}

		case m := <-messages:
			if m.Error != nil {
				log.Println(errors.Wrap(m.Error, "error from messages feed"))
			} else {
				if opts.batch {
					batchMessages <- m.Message
				} else {
					_, err := insertMessage(stmts, m.Message)
					if err != nil {
						log.Println(errors.Wrap(err, "could not insert message"))
					}
				}
				if opts.verbose {
					fmt.Println("Message: ", m.Message)
				}
			}

		case r := <-batchMessagesResult:
			if r.Error != nil {
				log.Println(errors.Wrap(err, "could not insert messages"))
			}
		}
	}
}

func getProductIDs(productIDs []string) ([]string, error) {
	allProductIDs := []string{
		"BTC-USD",
		"BTC-EUR",
		"BTC-GBP",
		"ETH-USD",
		"ETH-EUR",
		"ETH-BTC",
		"LTC-USD",
		"LTC-EUR",
		"LTC-BTC",
	}
	if len(productIDs) == 0 {
		return allProductIDs, nil
	}
	allProductIDsMap := make(map[string]bool)
	for _, p := range allProductIDs {
		allProductIDsMap[p] = true
	}

	for _, p := range productIDs {
		if p == "all" {
			return allProductIDs, nil
		}
		_, ok := allProductIDsMap[p]
		if !ok {
			return nil, fmt.Errorf("invalid product id: %s", p)
		}
	}

	return productIDs, nil
}
