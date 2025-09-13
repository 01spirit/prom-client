package main

import "github.com/01spirit/prom-client/client"

func main() {
	numSamples := 50
	numSeries := 100000
	sendNum := 50
	client.SampleGenerateAndSend(numSamples, numSeries, sendNum)
}
