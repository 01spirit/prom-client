package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/01spirit/prom-client/prompb"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
)

func SendSamples(numSamples, numSeries int) int64 {
	//numSamples := 1
	//numSeries := 1000000

	samples, series := createTimeseries(numSamples, numSeries, extraLabels...)

	client, err := InitWriteClient()
	if err != nil {
		panic(err)
	}

	queueManager := InitQueueManager(client)

	queueManager.StoreSeries(series, 0)

	var (
		pBuf = proto.NewBuffer(nil)
		buf  []byte
	)
	batch := make([]timeSeries, len(samples))
	for i, s := range samples {
		batch[i] = timeSeries{
			seriesLabels: queueManager.seriesLabels[s.Ref],
			metadata:     nil,
			timestamp:    s.T,
			value:        s.V,
		}
	}
	pendingData := make([]prompb.TimeSeries, len(samples))
	for i := range pendingData {
		pendingData[i].Samples = []prompb.Sample{{}}
	}
	nPendingSamples := populateTimeSeries(batch, pendingData)
	fmt.Println("pending samples: ", nPendingSamples)

	now := time.Now()

	err = queueManager.shards.sendSamples(context.Background(), pendingData, pBuf, &buf)
	if err != nil {
		panic(err)
	}

	lag := time.Since(now).Nanoseconds()
	fmt.Println("write latency: ", lag)

	return lag
}

func SendSingleSample(numSeries int, idx int) int64 {
	//numSamples := 1
	//numSeries := 1000000

	samples, series := createSingleTimeseries(numSeries, int64(idx))

	client, err := InitWriteClient()
	if err != nil {
		panic(err)
	}

	queueManager := InitQueueManager(client)

	queueManager.StoreSeries(series, 0)

	var (
		pBuf = proto.NewBuffer(nil)
		buf  []byte
	)
	batch := make([]timeSeries, len(samples))
	for i, s := range samples {
		batch[i] = timeSeries{
			seriesLabels: queueManager.seriesLabels[s.Ref],
			metadata:     nil,
			timestamp:    s.T,
			value:        s.V,
		}
	}
	pendingData := make([]prompb.TimeSeries, len(samples))
	for i := range pendingData {
		pendingData[i].Samples = []prompb.Sample{{}}
	}
	nPendingSamples := populateTimeSeries(batch, pendingData)
	fmt.Println("pending samples: ", nPendingSamples)

	now := time.Now()

	err = queueManager.shards.sendSamples(context.Background(), pendingData, pBuf, &buf)
	if err != nil {
		panic(err)
	}

	lag := time.Since(now).Nanoseconds()
	fmt.Println("write latency: ", lag)

	return lag
}

func SampleGenerateAndSend(numSamples, numSeries, sendNum int) {
	//numSamples := 50
	//numSeries := 100000
	//sendNum := 50

	queryString := `select * from node_cpu_seconds_total where time >= '2025-02-26T15:00:00Z' and time < '2025-02-26T16:00:00Z' group by cpu,mode,instance limit 10`
	resp, err := QueryFromInflux(queryString)
	if err != nil {
		panic(err)
	}

	fmt.Println("Influx query complete")

	client, err := InitWriteClient()
	if err != nil {
		panic(err)
	}

	queueManager := InitQueueManager(client)

	totalLag := int64(0)
	for i := 0; i < sendNum; i++ {
		samples, series, err := createTimeseriesByInflux(numSamples, numSeries, resp, i)
		if err != nil {
			panic(err)
		}

		queueManager.StoreSeries(series, 0)

		var (
			pBuf = proto.NewBuffer(nil)
			buf  []byte
		)
		batch := make([]timeSeries, len(samples))
		for i, s := range samples {
			batch[i] = timeSeries{
				seriesLabels: queueManager.seriesLabels[s.Ref],
				metadata:     nil,
				timestamp:    s.T,
				value:        s.V,
			}
		}
		pendingData := make([]prompb.TimeSeries, len(samples))
		for i := range pendingData {
			pendingData[i].Samples = []prompb.Sample{{}}
		}
		nPendingSamples := populateTimeSeries(batch, pendingData)

		now := time.Now()

		err = queueManager.shards.sendSamples(context.Background(), pendingData, pBuf, &buf)
		if err != nil {
			panic(err)
		}

		lag := time.Since(now).Nanoseconds()
		fmt.Printf("batch: %d , samples: %d , latency: %d ns\n", i, nPendingSamples, lag)
		totalLag += lag
	}
	fmt.Printf("\ntotal batch: %d , samples: %d , latency: %d ns\n", sendNum, numSeries*numSamples*sendNum, totalLag)
	fmt.Printf("average latency: %d ns\n", totalLag/int64(sendNum))
	//wg.Wait()

}

func Query(numSamples, numSeries int) {
	//numSamples := 30
	//numSeries := 50 * 1000

	var writeLatency int64 = 0
	for i := 0; i < numSamples; i++ {
		writeLatency += SendSingleSample(numSeries, i)
		//writeLatency += SendSamples(1, numSeries)
	}

	avgLatency := float64(writeLatency) / float64(numSamples) / 1e9
	fmt.Printf("\nInsert: series: %d , samples: %d , avg latency: %f ms, throughput: %f\n", numSeries, numSamples, avgLatency, float64(numSeries)/(avgLatency))
	//SendSamples(numSamples, numSeries)

	cli, err := InitQueryClient()
	if err != nil {
		panic(err)
	}

	for k := 0; k < 3; k++ {
		qryCnt := numSeries / 500

		var queryLatency int64 = 0
		for i := 0; i < qryCnt; i++ {
			matcher, _ := labels.NewMatcher(labels.MatchEqual, "__name__", fmt.Sprintf("metric_%d", k))
			matcher2, _ := labels.NewMatcher(labels.MatchEqual, fmt.Sprintf("label_%d", i*10+k), fmt.Sprintf("value_%d", i*10+k))

			var st, et int64
			st = 0
			et = int64(numSamples + 1)

			var query *prompb.Query
			if k == 1 {
				matcher1, _ := labels.NewMatcher(labels.MatchEqual, "__name__", "metric_11")
				query, _ = ToQuery(st, et, []*labels.Matcher{matcher1, matcher2}, nil)
				req := &prompb.ReadRequest{
					Queries:               []*prompb.Query{query},
					AcceptedResponseTypes: nil,
				}
				data, _ := proto.Marshal(req)
				compressed := snappy.Encode(nil, data)
				request, _ := http.NewRequest(http.MethodPost, RemoteQueryServer, bytes.NewBuffer(compressed))

				now := time.Now()

				response, _ := cli.Client.Do(request)
				defer response.Body.Close()

				body, _ := io.ReadAll(response.Body)
				uncompressed, _ := snappy.Decode(nil, body)

				resp := &prompb.ReadResponse{}
				_ = proto.Unmarshal(uncompressed, resp)

				lag := time.Since(now).Nanoseconds()
				queryLatency += lag
			}

			query, _ = ToQuery(st, et, []*labels.Matcher{matcher, matcher2}, nil)

			req := &prompb.ReadRequest{
				Queries:               []*prompb.Query{query},
				AcceptedResponseTypes: nil,
			}
			data, _ := proto.Marshal(req)
			compressed := snappy.Encode(nil, data)
			request, _ := http.NewRequest(http.MethodPost, RemoteQueryServer, bytes.NewBuffer(compressed))

			now := time.Now()

			response, _ := cli.Client.Do(request)
			defer response.Body.Close()

			body, _ := io.ReadAll(response.Body)
			uncompressed, _ := snappy.Decode(nil, body)

			resp := &prompb.ReadResponse{}
			_ = proto.Unmarshal(uncompressed, resp)

			lag := time.Since(now).Nanoseconds()
			queryLatency += lag
		}

		fmt.Printf("Query: series: %d , samples: %d , avg latency: %f ms\n", qryCnt, numSamples, float64(queryLatency)/float64(qryCnt)/1e6)
	}

}
