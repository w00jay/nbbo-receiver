package main

// k run --restart=Never --image=woojay/nbbo-receiver --port=2000 --expose receiver

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
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
	Symbol      string
	Time        string
	BidPrice    string
	BidExchange string
	AskPrice    string
	AskExchange string
}

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

const timeFormat = "03:04:05"

var nbboNow = nbboData{"", "00:00:00.001", "-1", "", "10000000", ""}

func main() {

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "{result: ok}")

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

			fmt.Println("= Same time group")

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

			// fmt.Println("NBBO now", nbboNow.Symbol, " : ", nbboNow.BidPrice, "@", nbboNow.AskPrice, "time: ", nbboNow.Time)

			//// If new time group, get set up for a new cycle
		} else if decodedTime.After(nbboNowTime) {

			// fmt.Println("+ New time group")
			// The nbboNow has the NBBO for the nbboNow.Symbol @ nbboNow.Time
			fmt.Println("NBBO ", nbboNow.Symbol, " : ", nbboNow.BidPrice, "@", nbboNow.AskPrice, "time: ", nbboNow.Time)

			// Reset the nbboNow
			nbboNow.Time = decoded.Time
			nbboNow.Symbol = decoded.Symbol
			nbboNow.BidPrice = decoded.BidPrice
			nbboNow.BidExchange = decoded.BidExchange
			nbboNow.AskPrice = decoded.AskPrice
			nbboNow.AskExchange = decoded.AskExchange
		}
	})

	log.Fatal(http.ListenAndServe(":2000", nil))

}
