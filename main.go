package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"os"

	"github.com/influxdata/telegraf/plugins/parsers/influx"
	influxSerializer "github.com/influxdata/telegraf/plugins/serializers/influx"
)

func hash(s []byte) uint32 {
	h := fnv.New32a()
	h.Write(s)
	return h.Sum32()
}

func makePostRequest(url string, metric string) {
	//fmt.Println("Posting to: " + url + " with metric: " + metric)
	_, err := http.Get(url)
	if err != nil {
		//fmt.Printf("error making http request: %s\n", err)
	} else {
		//fmt.Printf("http request status: %s\n", res.Status)
	}
}

func main() {
	metricToMonitor := os.Args[1]
	reportURL := os.Args[2]

	var last uint32

	parser := influx.NewStreamParser(os.Stdin)
	serializer := influxSerializer.Serializer{}
	if err := serializer.Init(); err != nil {
		fmt.Fprintf(os.Stderr, "serializer init failed: %v\n", err)
		os.Exit(1)
	}

	//fmt.Println("Monitoring for changes in metric: " + metricToMonitor + " and reporting to: " + reportURL)

	for {
		metric, err := parser.Next()
		if err != nil {
			if err == influx.EOF {
				return // stream ended
			}
			if parseErr, isParseError := err.(*influx.ParseError); isParseError {
				fmt.Fprintf(os.Stderr, "parse ERR %v\n", parseErr)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "ERR %v\n", err)
			os.Exit(1)
		}

		if metric.Name() == metricToMonitor {
			metric.Drop()

			fields, err := json.Marshal(metric.Fields())
			if err != nil {
				panic(err)
			}

			// hash the fields
			hashVariable := hash(fields)

			if hashVariable != last {
				last = hashVariable
				makePostRequest(reportURL, metricToMonitor)
			}
		} else {
			metric.Accept()
		}
		b, err := serializer.Serialize(metric)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERR %v\n", err)
			os.Exit(1)
		}
		fmt.Fprint(os.Stdout, string(b))
	}
}
