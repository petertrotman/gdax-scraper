package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/petertrotman/gdax-scraper/gdax"
	"github.com/pkg/errors"
)

// PrepareDB creates the necessary tables if they are not already in the database
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

type insertResult struct {
	Result sql.Result
	Error  error
}

// This is required because max_connections on gcloud is 100, and we easily exceed 100 connections otherwise
func batchInsertMessages(db *sql.DB, ch chan *gdax.Message) chan insertResult {
	t := time.NewTicker(1 * time.Second)
	var messages []*gdax.Message
	result := make(chan insertResult)

	go func() {
		for {
			<-ch
		}
	}()

	go func() {
		for {
			select {
			case msg := <-ch:
				messages = append(messages, msg)
			case <-t.C:
				var values []string
				for _, m := range messages {
					value := fmt.Sprintf(
						"('%v','%v',%v,'%v',%v,'%v','%v','%v',%v,%v,%v,%v,%v,'%v','%v','%v',%v,%v,%v,'%v')",
						m.Type,
						m.ProductId,
						m.TradeId,
						m.OrderId,
						m.Sequence,
						m.MakerOrderId,
						m.TakerOrderId,
						m.Time.Time().Format("2006-01-02T15:04:05.0000Z"), // exchange.Time() -> time.Time()
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
						m.Message.Message, // Because the Message is embedded?
					)
					values = append(values, value)
				}
				res, err := db.Exec(fmt.Sprintf(`
						INSERT INTO messages (
							type, product_id, trade_id, order_id, sequence, maker_order_id, taker_order_id, time, remaining_size, new_size, old_size, size, price, side, reason, order_type, funds, new_funds, old_funds, message
						) VALUES %s;
					`, strings.Join(values, ",")))

				// Reset the messages buffer
				messages = []*gdax.Message{}
				result <- insertResult{res, err}
			}
		}
	}()

	return result
}

func insertMessage(stmts *statements, m *gdax.Message) (sql.Result, error) {
	return stmts.InsertMessage.Exec(
		m.Type,
		m.ProductId,
		m.TradeId,
		m.OrderId,
		m.Sequence,
		m.MakerOrderId,
		m.TakerOrderId,
		m.Time.Time(), // exchange.Time() -> time.Time()
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
		m.Message.Message, // Because the Message is embedded?
	)
}

type parsedBidAsk struct {
	Price   float64
	Size    float64
	OrderID string
}

func batchInsertSnapshot(db *sql.DB, s *gdax.Snapshot) (sql.Result, error) {
	parseBidAsk := func(vals []string) (*parsedBidAsk, error) {
		price, err := strconv.ParseFloat(vals[0], 64)
		if err != nil {
			return nil, errors.Wrap(err, "could not convert price to float")
		}
		size, err := strconv.ParseFloat(vals[1], 64)
		if err != nil {
			return nil, errors.Wrap(err, "could not convert size to float")
		}
		return &parsedBidAsk{price, size, vals[2]}, nil
	}

	var values []string
	for _, bid := range s.Bids {
		b, err := parseBidAsk(bid)
		if err != nil {
			return nil, errors.Wrap(err, "could not parse bid")
		}
		value := fmt.Sprintf("('%v',%v,'%v',%v,%v,'%v')",
			s.ProductID,
			s.Sequence,
			"bid",
			b.Price,
			b.Size,
			b.OrderID,
		)
		values = append(values, value)
	}
	for _, ask := range s.Asks {
		a, err := parseBidAsk(ask)
		if err != nil {
			return nil, errors.Wrap(err, "could not parse ask")
		}
		value := fmt.Sprintf("('%v',%v,'%v',%v,%v,'%v')",
			s.ProductID,
			s.Sequence,
			"ask",
			a.Price,
			a.Size,
			a.OrderID,
		)
		values = append(values, value)
	}

	return db.Exec(fmt.Sprintf(`
		INSERT INTO snapshots (
			product_id, sequence, bid_ask, price, size, order_id
		) VALUES %s;
	`, strings.Join(values, ",")))
}

func insertSnapshot(stmts *statements, s *gdax.Snapshot) ([]sql.Result, error) {
	parseBidAsk := func(vals []string) (*parsedBidAsk, error) {
		price, err := strconv.ParseFloat(vals[0], 64)
		if err != nil {
			return nil, errors.Wrap(err, "could not convert price to float")
		}
		size, err := strconv.ParseFloat(vals[1], 64)
		if err != nil {
			return nil, errors.Wrap(err, "could not convert size to float")
		}
		return &parsedBidAsk{price, size, vals[2]}, nil
	}

	var results []sql.Result

	for _, bid := range s.Bids {
		b, err := parseBidAsk(bid)
		if err != nil {
			return nil, errors.Wrap(err, "could not parse bid")
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
			return nil, errors.Wrap(err, "could not insert bid")
		}
		results = append(results, result)
	}

	for _, ask := range s.Asks {
		a, err := parseBidAsk(ask)
		if err != nil {
			return nil, errors.Wrap(err, "could not parse ask")
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
			return nil, errors.Wrap(err, "could not insert ask")
		}
		results = append(results, result)
	}

	return results, nil
}
