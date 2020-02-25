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
	AskPrice    string `json:"askprice,omitempty"`
	AskExchange string `json:"askexchange,omitempty"`
}

const timeFormat = "03:04:05"

var nbboNow = nbboData{"", "00:00:00.001", "-1", "", "10000000", ""}
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
	client := redis.NewClient(&redis.Options{
		Addr: "redis-master.default.svc.cluster.local:6379",
		// Password: "", // no password set
		DialTimeout: 10 * time.Second,
		MaxRetries:  3,
		DB:          0, // use default DB
	})
	err := client.Set("testkey", "testvalue", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := client.Get("testkey").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("testkey", val)

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

			//// If new time, get set up for a new time group
		} else if decodedTime.After(nbboNowTime) {

			// The nbboNow has the NBBO for the nbboNow.Symbol @ nbboNow.Time
			fmt.Println("NBBO ", nbboNow.Symbol, " : ", nbboNow.BidPrice, "@", nbboNow.AskPrice, "time: ", nbboNow.Time)

			// Save the latest NBBO
			msg, err := json.Marshal(&nbboNow)
			if err != nil {
				panic(err)
			}

			sendErr := client.Set(nbboNow.Symbol, msg, 0).Err()
			if err != nil {
				panic(sendErr)
			}

			// err := client.Set(nbboNow.Symbol, nbboNow.BidPrice, 0).Err()
			// if err != nil {
			// 	panic(err)
			// }

			// Debug REDIS
			val, err := client.Get(nbboNow.Symbol).Result()
			if err != nil {
				panic(err)
			}
			temp := []byte(val)
			decodedMsg := nbboData{}
			err = json.Unmarshal(temp, &decodedMsg)
			if err != nil {
				return
			}
			fmt.Println("NBBO Decoded", decodedMsg.Symbol, " \t: ", decodedMsg.BidPrice, "@", decodedMsg.AskPrice, "time: ", decodedMsg.Time)

			// Reset the nbboNow
			nbboNow.Time = decoded.Time
			nbboNow.Symbol = decoded.Symbol
			nbboNow.BidPrice = decoded.BidPrice
			nbboNow.BidExchange = decoded.BidExchange
			nbboNow.AskPrice = decoded.AskPrice
			nbboNow.AskExchange = decoded.AskExchange
		}
	})

	// Prometheus
	// k port-forward svc/pro-prometheus-server  9090:80
	http.Handle("/metrics", promhttp.Handler())

	log.Fatal(http.ListenAndServe(":2000", nil))
}
