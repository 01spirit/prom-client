package main

import "github.com/01spirit/prom-client/client"

func main() {
	numSamples := 30
	numSeries := 50 * 1000
	client.Query(numSamples, numSeries)
}
