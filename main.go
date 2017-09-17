package main

import (
	"database/sql"
	"os"
	"strconv"

	_ "github.com/lib/pq"
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
		cli.BoolFlag{
			Name:  "verbose, v",
			Usage: "increased logging to stdout",
		},
	}
	app.Action = func(c *cli.Context) error {
		err := run(&runOpts{
			databaseURI: c.String("database"),
			verbose:     c.Bool("verbose"),
		})
		if err != nil {
			return cli.NewExitError(err, 1)
		}
		return nil
	}
	app.Run(os.Args)
}

type runOpts struct {
	databaseURI string
	verbose     bool
}

func run(opts *runOpts) error {
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

	_ = stmts

	return nil
}

func prepareDB(db *sql.DB) error {
	var err error

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS messages (
		id BIGSERIAL PRIMARY KEY,
		type VARCHAR(30),
		product_id VARCHAR(10),
		trade_id BIGINT,
		order_id VARCHAR(60),
		sequence BIGINT,
		maker_order_id VARCHAR(60),
		taker_order_id VARCHAR(60),
		time TIMESTAMP,
		remaining_size NUMERIC,
		new_size NUMERIC,
		old_size NUMERIC,
		size NUMERIC,
		price NUMERIC,
		side VARCHAR(10),
		reason VARCHAR(60),
		order_type VARCHAR(10),
		funds NUMERIC,
		new_funds NUMERIC,
		old_funds NUMERIC,
		message VARCHAR(60)
	);
	`)
	if err != nil {
		return errors.Wrap(err, "could not create messages table")
	}

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS snapshots (
		id BIGSERIAL PRIMARY KEY,
		product_id VARCHAR(10),
		sequence BIGINT,
		bid_ask VARCHAR(3),
		price NUMERIC,
		size NUMERIC,
		order_id VARCHAR(60)
	);
	`)
	if err != nil {
		return errors.Wrap(err, "could not create snapshots table")
	}

	return nil
}

type statements struct {
	InsertMessage  *sql.Stmt
	InsertSnapshot *sql.Stmt
}

func prepareStmts(db *sql.DB) (*statements, error) {
	insertMessageStmt, err := db.Prepare(`
	INSERT INTO messages (
		type, product_id, trade_id, order_id, sequence, maker_order_id, taker_order_id, time, remaining_size, new_size, old_size, size, price, side, reason, order_type, funds, new_funds, old_funds, message
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
	);
	`)
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare insert message statement")
	}

	insertSnapshotStmt, err := db.Prepare(`
	INSERT INTO snapshots (
		product_id, sequence, bid_ask, price, size, order_id
	) VALUES (
		$1, $2, $3, $4, $5, $6
	);
	`)
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare insert snapshot statement")
	}

	stmts := statements{
		InsertMessage:  insertMessageStmt,
		InsertSnapshot: insertSnapshotStmt,
	}
	return &stmts, nil
}

func insertMessage(stmts *statements, m *Message) (sql.Result, error) {
	return stmts.InsertMessage.Exec(
		m.Type,
		m.ProductId,
		m.TradeId,
		m.OrderId,
		m.Sequence,
		m.MakerOrderId,
		m.TakerOrderId,
		m.Time,
		m.RemainingSize,
		m.NewSize,
		m.OldSize,
		m.Size,
		m.Price,
		m.Side,
		m.Reason,
		m.OrderType,
		m.Funds,
		m.NewFunds,
		m.OldFunds,
		m.Message,
	)
}

type parsedBidAsk struct {
	Price   float64
	Size    float64
	OrderID string
}

func insertSnapshot(stmts *statements, s *Snapshot) ([]sql.Result, error) {
	parseBidAsk := func(bidask string, vals []string) (*parsedBidAsk, error) {
		price, err := strconv.ParseFloat(vals[0])
		if err != nil {
			return nil, errors.Wrap(err, "could not convert price to float")
		}
		size, err := strconv.ParseFloat(vals[1])
		if err != nil {
			return nil, errors.Wrap(err, "could not convert size to float")
		}
		return &parsedBidAsk{price, size, vals[2]}
	}

	var results []sql.Result

	for _, bid := range s.Bids {
		b, err := parseBidAsk
		if err != nil {
			return nil, errors.Wrap("could not parse bid")
		}
		result, err := stmts.InsertSnapshot.Exec(
			s.ProductID,
			s.Sequence,
			"bid",
			b.Price,
			b.Size,
			b.OrderID,
		)
		if err != nil {
			return nil, errors.Wrap("could not insert bid")
		}
		results = append(results, result)
	}

	for _, ask := range s.Asks {
		a, err := parseBidAsk
		if err != nil {
			return nil, errors.Wrap("could not parse ask")
		}
		result, err := stmts.InsertSnapshot.Exec(
			s.ProductID,
			s.Sequence,
			"ask",
			a.Price,
			a.Size,
			a.OrderID,
		)
		if err != nil {
			return nil, errors.Wrap("could not insert ask")
		}
		results = append(results, result)
	}

	return results, nil
}
