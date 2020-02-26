package main

// k run --restart=Never --image=woojay/nbbo-receiver --port=2000 --expose receiver

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type msgFormat struct {
	Symbol      string
	Date        string
	Time        string
	BidPrice    string
	BidExchange string
	BidSize     string
	AskPrice    string
	AskExchange string
	AskSize     string
}

type nbboData struct {
	Symbol      string `json:"symbol,omitempty"`
	Time        string `json:"time,omitempty"`
	BidPrice    string `json:"bidprice,omitempty"`
	BidExchange string `json:"bidexchange,omitempty"`
	BidSize     string `json:"bidsize,omitempty"`
	AskPrice    string `json:"askprice,omitempty"`
	AskExchange string `json:"askexchange,omitempty"`
	AskSize     string `json:"asksize,omitempty"`
}

const timeFormat = "03:04:05"

var nbboNow = nbboData{"", "00:00:00.001", "-1", "", "", "10000000", "", ""}
var nbboStore = map[string]nbboData{}

var nbboProcessed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "receiver_processed_nbbo_total",
	Help: "The total number of processed events",
})

func decode(s string) msgFormat {
	t := strings.Split(s, ",")

	return msgFormat{
		Symbol:      t[0],
		Date:        t[1],
		Time:        t[2],
		BidPrice:    t[3],
		BidExchange: t[4],
		BidSize:     t[5],
		AskPrice:    t[6],
		AskExchange: t[7],
		AskSize:     t[8],
	}
}

func main() {

	// Start REDIS
	rdsClient := redis.NewClient(&redis.Options{
		Addr: "redis-master.default.svc.cluster.local:6379",
		// Password: "", // no password set
		DialTimeout: 10 * time.Second,
		MaxRetries:  3,
		DB:          0, // use default DB
	})
	pushRes := rdsClient.RPush("testkey", "testvalue")
	if pushRes.Err() != nil {
		panic(pushRes.Err())
	}

	popRes := rdsClient.RPop("testkey")
	if popRes.Err() != nil {
		panic(popRes.Err())
	}
	fmt.Println("testkey", popRes.Val())

	// Serve / route
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "{result: ok}")

		// Prom counter
		nbboProcessed.Inc()

		// Get
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal("Error reading response. ", err)
		}

		// Dump
		sb := string(body)
		decoded := decode(sb)
		fmt.Println(decoded.BidPrice, "@", decoded.AskPrice, "--", sb)

		// Process
		//// If in the same time group, find the best
		decodedTime, _ := time.Parse(timeFormat, decoded.Time)
		nbboNowTime, _ := time.Parse(timeFormat, nbboNow.Time)

		if decodedTime == nbboNowTime {

			// fmt.Println("= Same time group")

			decodedBidPrice, _ := strconv.ParseFloat(decoded.BidPrice, 64)
			nbboNowBidPrice, _ := strconv.ParseFloat(nbboNow.BidPrice, 64)
			decodedAskPrice, _ := strconv.ParseFloat(decoded.AskPrice, 64)
			nbboNowAskPrice, _ := strconv.ParseFloat(nbboNow.AskPrice, 64)

			// fmt.Println("Bid: ", decodedBidPrice, nbboNowBidPrice)
			// fmt.Println("Ask: ", decodedAskPrice, nbboNowAskPrice)

			// Get better (higher) Bid
			if decodedBidPrice > nbboNowBidPrice {
				fmt.Println("Higher bid price @", decoded.BidPrice, " vs ", nbboNow.BidPrice)
				nbboNow.BidPrice = decoded.BidPrice
				nbboNow.BidExchange = decoded.BidExchange
			}

			// Get better (lower) Offer/Ask
			if decodedAskPrice < nbboNowAskPrice {
				fmt.Println("Lower ask price @", decoded.AskPrice, " vs ", nbboNow.AskPrice)
				nbboNow.AskPrice = decoded.AskPrice
				nbboNow.AskExchange = decoded.AskExchange
			}

			nbboNow.BidSize = decoded.BidSize
			nbboNow.AskSize = decoded.AskSize

			//// If new time, get set up for a new time group
		} else if decodedTime.After(nbboNowTime) {

			// The nbboNow has the NBBO for the nbboNow.Symbol @ nbboNow.Time
			fmt.Println("NBBO ", nbboNow.Symbol, " : ", nbboNow.BidPrice, "@", nbboNow.AskPrice, "time: ", nbboNow.Time)

			// Save the latest NBBO
			msg, err := json.Marshal(&nbboNow)
			if err != nil {
				panic(err)
			}

			pushRes := rdsClient.RPush(nbboNow.Symbol, msg)
			if pushRes.Err() != nil {
				panic(pushRes.Err())
			}

			// err := rdsClient.Set(nbboNow.Symbol, nbboNow.BidPrice, 0).Err()
			// if err != nil {
			// 	panic(err)
			// }

			// Reset the nbboNow
			nbboNow.Time = decoded.Time
			nbboNow.Symbol = decoded.Symbol
			nbboNow.BidPrice = decoded.BidPrice
			nbboNow.BidExchange = decoded.BidExchange
			nbboNow.BidSize = decoded.BidSize
			nbboNow.AskPrice = decoded.AskPrice
			nbboNow.AskExchange = decoded.AskExchange
			nbboNow.AskPrice = decoded.AskPrice
		}
	})

	http.HandleFunc("/fill", func(w http.ResponseWriter, r *http.Request) {

		q := r.URL.Query()

		reqSymbols, ok := q["symbol"]
		if !ok || len(reqSymbols) == 0 {
			fmt.Fprintf(w, "Missing symbol"+r.URL.RequestURI()) // Pseudo 400
			log.Println("Missing symbol")
		}

		reqBids, ok := q["bid"]
		if !ok || len(reqBids) == 0 {
			fmt.Fprintf(w, "Missing bid price"+r.URL.RequestURI())
			log.Println("Missing bid")
		}

		reqQuantities, ok := q["quantity"]
		if !ok || len(reqQuantities) == 0 {
			fmt.Fprintf(w, "Missing quantity"+r.URL.RequestURI())
			log.Println("Missing quantity")
		}

		reqSymbol := reqSymbols[0]
		reqBid, _ := strconv.ParseFloat(reqBids[0], 64)
		reqQuantity, _ := strconv.ParseInt(reqQuantities[0], 10, 64)
		fillOrders := []nbboData{}
		filledQuantity := int64(0)

		// Get Symbol
		// keys := rdsClient.Keys("*")
		listLen := rdsClient.LLen(reqSymbol).Val() // thread unsafe
		if listLen == 0 {
			w.WriteHeader(200)
			w.Write([]byte("Symbol not found"))
		} else {
			for {
				// Get Price
				//// Get last NBBO
				lastNbboRaw := rdsClient.LRange(reqSymbol, listLen-1, listLen-1)

				// TODO:
				// check we are not running out of NBBOs before moving on

				temp := []byte(lastNbboRaw.Val()[0])
				decodedMsg := nbboData{}
				err := json.Unmarshal(temp, &decodedMsg)
				if err != nil {
					return
				}

				// If offer is less than or equal to willing bid, buy/fill
				ask, _ := strconv.ParseFloat(decodedMsg.AskPrice, 64)
				// bid, _ := strconv.ParseFloat(reqBid, 64)
				if ask <= reqBid {
					// Get Quantity -> will overfill before termination for now
					askSize, _ := strconv.ParseInt(decodedMsg.AskSize, 10, 64)
					filledQuantity += askSize

					// Save the quote
					fillOrders = append(fillOrders, decodedMsg)

					// if quantity is matched or overfilled, done
					if filledQuantity >= reqQuantity {
						w.WriteHeader(200)
						w.Write([]byte("Good query received:" + r.URL.RawQuery + "<br>" + "Symbol: " + reqSymbols[0] + " Bid: " + reqBids[0] + " Qty: " + reqQuantities[0] + "<P>"))

						// Report each quotes in the fill
						total := int64(0)
						for _, fill := range fillOrders {
							fillAsk, _ := strconv.ParseInt(fill.AskSize, 10, 32)
							total += fillAsk
							w.Write([]byte("@" + fill.Time + " Bid Price: " + fill.BidPrice + " Size: " + fill.BidSize + " Ask Price: " + fill.AskPrice + " Size: " + fill.AskSize + " Cumulative: " + string(total) + "<BR>"))
							log.Println("@" + fill.Time + " Bid Price: " + fill.BidPrice + " Size: " + fill.BidSize + " Ask Price: " + fill.AskPrice + " Size: " + fill.AskSize + " Cumulative: " + strconv.Itoa(total))
						}

						log.Println("Good query received:" + r.URL.RawQuery + "<br>" + "Symbol: " + reqSymbols[0] + " Bid: " + reqBids[0] + " Qty: " + reqQuantities[0])

						break
					}
				}
			}
		}

	})

	// Prometheus
	// k port-forward svc/pro-prometheus-server  9090:80
	http.Handle("/metrics", promhttp.Handler())

	defer rdsClient.Close()
	log.Fatal(http.ListenAndServe(":2000", nil))
}
