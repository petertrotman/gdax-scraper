package gdax

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"time"

	ws "github.com/gorilla/websocket"
	"github.com/pkg/errors"
	exchange "github.com/preichenberger/go-coinbase-exchange"
)

// Message is the type of the received message through the Subscribe chans
type Message struct {
	exchange.Message
}

// Channel is a descriptor of a channel we want to receive.
type Channel struct {
	Name       string   `json:"name"`
	ProductIDs []string `json:"product_ids"`
}

// SubscribeMessage is the initial message sent to the server detailing the channels we want to receive.
type subscribeMessage struct {
	Type     string    `json:"type"`
	Channels []Channel `json:"channels"`
}

// MessageResult is the type returned through the Subscribe channel
type MessageResult struct {
	Message *Message
	Error   error
}

// Subscribe subscribes to the supplied API Channels and returns chans which produce output
func Subscribe(channels []Channel) (chan MessageResult, error) {
	dialer := ws.Dialer{}
	conn, _, err := dialer.Dial("wss://ws-feed.gdax.com", nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not open websocket connection")
	}

	subscribe := subscribeMessage{"subscribe", channels}
	if err = conn.WriteJSON(subscribe); err != nil {
		return nil, errors.Wrap(err, "could not write subscription:")
	}

	ch := make(chan MessageResult)
	go func() {
		for {
			message := Message{}
			if err := conn.ReadJSON(&message); err != nil {
				ch <- MessageResult{nil, errors.Wrap(err, "could not read received message")}
			}
			ch <- MessageResult{&message, nil}
		}
	}()

	return ch, nil
}

// OrderBook is the struct returned by the snapshot
type OrderBook struct {
	Sequence int64      `json:"sequence"`
	Bids     [][]string `json:"bids"` // [[price, size, order_id], ...]
	Asks     [][]string `json:"asks"` // [[price, size, order_id], ...]
}

// Snapshot returns the current snapshot of the orderbook for the given productID
func Snapshot(productID string) (*OrderBook, error) {
	addr := fmt.Sprintf("https://api.gdax.com/products/%s/book?level=3", productID)
	client := &http.Client{Timeout: time.Second * 3}

	response, err := client.Get(addr)
	if err != nil {
		return nil, errors.Wrap(err, "could not get the order book")
	}
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("invalid response from server: %s", response.Status)
	}
	defer response.Body.Close()

	buf, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, errors.Wrap(err, "could not read from response body")
	}

	var orderBook OrderBook
	if err = json.Unmarshal(buf, &orderBook); err != nil {
		return nil, errors.Wrap(err, "could not decode response body")
	}

	return &orderBook, nil
}

// OrderBookResult is the type returned throught eh Snapshots channel
type OrderBookResult struct {
	OrderBooks map[string]*OrderBook
	Errors     []error
}

type productIDOrderBook struct {
	ProductID string
	OrderBook *OrderBook
}

// Snapshots retrieves all the snapshots for the given product IDs, whilst respecting API rate limits
func Snapshots(productIDs []string) <-chan OrderBookResult {
	const requestsPerSecond int = 3 // GDAX API rate limit

	ticker := time.NewTicker(1 * time.Second)
	resultChan := make(chan OrderBookResult)
	resCh := make(chan productIDOrderBook)
	errCh := make(chan error)

	go func() {
		for i := 0; i <= int(len(productIDs)/requestsPerSecond); i++ {
			var end = int(math.Min(float64(i+requestsPerSecond), float64(len(productIDs))))
			for _, productID := range productIDs[i:end] {
				go func(_productID string) {
					orderBook, err := Snapshot(_productID)
					if err != nil {
						errCh <- err
					}
					resCh <- productIDOrderBook{_productID, orderBook}
				}(productID)
			}
			<-ticker.C
		}
	}()

	go func() {
		var errs []error
		res := make(map[string]*OrderBook)
	loop:
		for {
			select {
			case r := <-resCh:
				res[r.ProductID] = r.OrderBook
				if len(res) == len(productIDs) {
					break loop
				}
			case err := <-errCh:
				errs = append(errs, err)
			}
		}
		resultChan <- OrderBookResult{res, errs}
	}()

	return resultChan
}
