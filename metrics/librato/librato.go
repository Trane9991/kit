package librato

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/generic"
	"github.com/go-kit/kit/metrics/internal/lv"
)

const (
	maxConcurrentRequests = 20
	maxValuesInABatch     = 300
	metricsURL            = "https://metrics-api.librato.com/v1/metrics"
)

type Percentiles []struct {
	s string
	f float64
}

// Librato receives metrics observations and forwards them to Librato.
// Create a Librato object, use it to create metrics, and pass those metrics as
// dependencies to the components that will use them.
//
// To regularly report metrics to Librato, use the WriteLoop helper method.
type Librato struct {
	user, token           string
	mtx                   sync.RWMutex
	sem                   chan struct{}
	client                *http.Client
	counters              *lv.Space
	gauges                *lv.Space
	histograms            *lv.Space
	averages              *lv.Space
	percentiles           []float64 // percentiles to track
	logger                log.Logger
	numConcurrentRequests int
	maxBatchSize          int
}

type Metric struct {
	MeasureTime *int64  `json:"measure_time"`
	Name        string  `json:"name"`
	Value       float64 `json:"value"`
	Source      *string `json:"string"`
}

type Option func(*Librato)

func (lb *Librato) apply(opt Option) {
	if opt != nil {
		opt(lb)
	}
}

func WithLogger(logger log.Logger) Option {
	return func(lb *Librato) {
		lb.logger = logger
	}
}

// WithPercentiles registers the percentiles to track, overriding the
// existing/default values.
// Reason is that Librato makes you pay per metric, so you can save half the money
// by only using 2 metrics instead of the default 4.
func WithPercentiles(percentiles ...float64) Option {
	return func(lb *Librato) {
		lb.percentiles = make([]float64, 0, len(percentiles))
		for _, p := range percentiles {
			if p < 0 || p > 1 {
				continue // illegal entry; ignore
			}
			lb.percentiles = append(lb.percentiles, p)
		}
	}
}

func WithConcurrentRequests(n int) Option {
	return func(lb *Librato) {
		if n > maxConcurrentRequests {
			n = maxConcurrentRequests
		}
		lb.numConcurrentRequests = n
	}
}

// WithMaxBatchSize sets custom max batch size, default and maximum is 300
// Bigger batches may result in "HTTP 413 - Request Entity Too Large" response
// If more then measurements will be collected, data will be batched
// into multiple requests
func WithMaxBatchSize(n int) Option {
	return func(lb *Librato) {
		if n > maxValuesInABatch {
			n = maxValuesInABatch
		}
		lb.maxBatchSize = n
	}
}

func WithHttpClient(c *http.Client) Option {
	return func(lb *Librato) {
		lb.client = c
	}
}

// New returns a Librato object that may be used to create metrics.
// Callers must ensure that regular calls to Send are performed, either
// manually or with one of the helper methods.
func New(user, token string, options ...Option) *Librato {
	lb := &Librato{
		user: user, token: token,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		counters:              lv.NewSpace(),
		gauges:                lv.NewSpace(),
		histograms:            lv.NewSpace(),
		averages:              lv.NewSpace(),
		logger:                log.NewLogfmtLogger(os.Stderr),
		percentiles:           []float64{0.50, 0.90, 0.95, 0.99},
		numConcurrentRequests: maxConcurrentRequests,
		maxBatchSize:          maxValuesInABatch,
	}

	for _, optFunc := range options {
		optFunc(lb)

	}
	lb.sem = make(chan struct{}, lb.numConcurrentRequests)

	return lb
}

// NewCounter returns a counter. Observations are aggregated and emitted once
// per write invocation.
func (lb *Librato) NewCounter(name string) metrics.Counter {
	return &Counter{
		name: name,
		obs:  lb.counters.Observe,
	}
}

// NewGauge returns an gauge.
func (lb *Librato) NewGauge(name string) metrics.Gauge {
	return &Gauge{
		name: name,
		obs:  lb.gauges.Observe,
		add:  lb.gauges.Add,
	}
}

// NewAvgGauge returns Gauge which will calculate average of submitted values
// before sending them to librato
func (lb *Librato) NewAvgGauge(name string) metrics.Histogram {
	return &Histogram{
		name: name,
		obs:  lb.averages.Observe,
	}
}

// NewHistogram returns a histogram.
func (lb *Librato) NewHistogram(name string) metrics.Histogram {
	return &Histogram{
		name: name,
		obs:  lb.histograms.Observe,
	}
}

// WriteLoop is a helper method that invokes Send every time the passed
// channel fires. This method blocks until ctx is canceled, so clients
// probably want to run it in its own goroutine. For typical usage, create a
// time.Ticker and pass its C channel to this method.
func (lb *Librato) WriteLoop(ctx context.Context, c <-chan time.Time) {
	for {
		select {
		case <-c:
			if err := lb.Send(); err != nil {
				lb.logger.Log("during", "Send", "msg", "Failed to send librato metrics", "err", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// Send will fire an API request to Librato with the latest stats for
// all metrics. It is preferred that the WriteLoop method is used.
func (lb *Librato) Send() error {
	lb.mtx.RLock()
	defer lb.mtx.RUnlock()
	now := time.Now().Unix()

	var batches []map[string][]Metric
	datums := map[string][]Metric{}

	add := func(typ string, m Metric) {
		count := 0
		for _, v := range datums {
			count += len(v)
		}
		if count >= lb.maxBatchSize {
			batches = append(batches, datums)
			datums = map[string][]Metric{}
		}

		datums[typ] = append(datums[typ], m)
	}
	addCounter := func(m Metric) { add("counters", m) }
	addGauge := func(m Metric) { add("gauges", m) }

	lb.counters.Reset().Walk(func(name string, lvs lv.LabelValues, values []float64) bool {
		addCounter(Metric{
			Value:       sum(values),
			MeasureTime: &now,
			Name:        name,
			Source:      getSourceFromLabels(lvs),
		})
		return true
	})

	lb.gauges.Reset().Walk(func(name string, lvs lv.LabelValues, values []float64) bool {
		if len(values) == 0 {
			return true
		}

		for _, v := range values {
			addGauge(Metric{
				Value:       v,
				MeasureTime: &now,
				Name:        name,
				Source:      getSourceFromLabels(lvs),
			})
		}

		return true
	})

	// format a [0,1]-float value to a percentile value, with minimum nr of decimals
	// 0.90 -> "90"
	// 0.95 -> "95"
	// 0.999 -> "99.9"
	formatPerc := func(p float64) string {
		return strconv.FormatFloat(p*100, 'f', -1, 64)
	}

	lb.histograms.Reset().Walk(func(name string, lvs lv.LabelValues, values []float64) bool {
		histogram := generic.NewHistogram(name, 50)

		for _, v := range values {
			histogram.Observe(v)
		}

		for _, perc := range lb.percentiles {
			value := histogram.Quantile(perc)
			addGauge(Metric{
				Value:       value,
				MeasureTime: &now,
				Name:        fmt.Sprintf("%s_%s", name, formatPerc(perc)),
				Source:      getSourceFromLabels(lvs),
			})
		}
		return true
	})

	lb.averages.Reset().Walk(func(name string, lvs lv.LabelValues, values []float64) bool {
		avg := generic.NewSimpleHistogram()
		for _, v := range values {
			avg.Observe(v)
		}

		addGauge(Metric{
			Value:       avg.ApproximateMovingAverage(),
			MeasureTime: &now,
			Name:        name,
			Source:      getSourceFromLabels(lvs),
		})
		return true
	})

	batches = append(batches, datums)

	c := 0
	var errors = make(chan error, len(batches))
	for _, batch := range batches {

		for _, v := range batch {
			c += len(v)
		}
		go func(batch map[string][]Metric) {
			lb.sem <- struct{}{}
			defer func() {
				<-lb.sem
			}()

			errors <- lb.postMetric(batch)
		}(batch)
	}

	var firstErr error
	for i := 0; i < cap(errors); i++ {
		if err := <-errors; err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (lb *Librato) postMetric(body map[string][]Metric) error {
	b, err := json.Marshal(body)
	if nil != err {
		return err
	}

	return lb.makeRequest(bytes.NewBuffer(b), metricsURL)
}

func (lb *Librato) makeRequest(data *bytes.Buffer, url string) error {
	req, err := http.NewRequest(http.MethodPost, url, data)
	if nil != err {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(lb.user, lb.token)
	res, err := lb.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// http://api-docs-archive.librato.com/#http-status-codes
	if res.StatusCode <= 204 {
		io.Copy(ioutil.Discard, res.Body)
	} else {
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("Librato response status %d, error %s", res.StatusCode, string(b))
	}

	return nil
}

func sum(a []float64) float64 {
	var v float64
	for _, f := range a {
		v += f
	}
	return v
}

func last(a []float64) float64 {
	return a[len(a)-1]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type observeFunc func(name string, lvs lv.LabelValues, value float64)

// Counter is a counter. Observations are forwarded to a node
// object, and aggregated (summed) per timeseries.
type Counter struct {
	name string
	lvs  lv.LabelValues
	obs  observeFunc
}

// With implements metrics.Counter.
func (c *Counter) With(labelValues ...string) metrics.Counter {
	return &Counter{
		name: c.name,
		lvs:  c.lvs.With(labelValues...),
		obs:  c.obs,
	}
}

// Add implements metrics.Counter.
func (c *Counter) Add(delta float64) {
	c.obs(c.name, c.lvs, delta)
}

// Gauge is a gauge. Observations are forwarded to a node
// object, and aggregated (the last observation selected) per timeseries.
type Gauge struct {
	name string
	lvs  lv.LabelValues
	obs  observeFunc
	add  observeFunc
}

// With implements metrics.Gauge.
func (g *Gauge) With(labelValues ...string) metrics.Gauge {
	return &Gauge{
		name: g.name,
		lvs:  g.lvs.With(labelValues...),
		obs:  g.obs,
		add:  g.add,
	}
}

// Set implements metrics.Gauge.
func (g *Gauge) Set(value float64) {
	g.obs(g.name, g.lvs, value)
}

// Add implements metrics.Gauge.
func (g *Gauge) Add(delta float64) {
	g.add(g.name, g.lvs, delta)
}

// Histogram is an Influx histrogram. Observations are aggregated into a
// generic.Histogram and emitted as per-quantile gauges to the Influx server.
type Histogram struct {
	name string
	lvs  lv.LabelValues
	obs  observeFunc
}

// With implements metrics.Histogram.
func (h *Histogram) With(labelValues ...string) metrics.Histogram {
	return &Histogram{
		name: h.name,
		lvs:  h.lvs.With(labelValues...),
		obs:  h.obs,
	}

}

// Observe implements metrics.Histogram.
func (h *Histogram) Observe(value float64) {
	h.obs(h.name, h.lvs, value)
}

// GetSourceFromLabels fetches "source" from labels
func getSourceFromLabels(lvs lv.LabelValues) *string {
	for i, l := range lvs {
		if l == "source" && i+1 < len(lvs) {
			return &lvs[i+1]
		}
	}
	return nil
}
