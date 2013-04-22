package exporter

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang"
	"github.com/prometheus/client_golang/metrics"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	proto = "tcp"
)

var verbose = flag.Bool("verbose", false, "Verbose output.")

// For unmarshaling config file.
type config struct {
	Endpoints             []string `json:"endpoints"`
	ListeningAddress      string   `json:"listeningAddress"`
	MetricsEndpoint       string   `json:"metricsEndpoint"`
	GangliaScrapeInterval int      `json:"gangliaScrapeInterval"`
}

// Prints debug information if verbose flag is set.
func debug(format string, a ...interface{}) {
	if *verbose {
		log.Printf(format, a...)
	}
}

func toUtf8(charset string, input io.Reader) (io.Reader, error) {
	return input, nil //FIXME
}

// The gmond exporter.
type exporter struct {
	sync.RWMutex
	Metrics               map[string]metrics.Gauge
	configFile            string
	listeningAddress      string
	gangliaScrapeInterval time.Duration

	verbose         bool
	metricsEndpoint string
	conf            config
	configChan      chan config

	scrapeDuration  metrics.Histogram
	metricsUpdated  metrics.Gauge
	metricsExported metrics.Gauge
}

func (e *exporter) setMetric(name string, labels map[string]string, metric Metric) {
	debug("%s{%s} = %f", name, labels, metric.Value)
	e.Lock()
	defer e.Unlock()
	if _, ok := e.Metrics[name]; !ok {
		var desc string
		var title string
		for _, element := range metric.ExtraData.ExtraElements {
			switch element.Name {
			case "DESC":
				desc = element.Val
			case "TITLE":
				title = element.Val
			}
			if title != "" && desc != "" {
				break
			}
		}
		debug("New metric: %s (%s)", name, desc)
		gauge := metrics.NewGauge()
		e.Metrics[name] = gauge
		registry.Register(name, desc, registry.NilLabels, gauge) // one gauge per metric!
	}
	e.Metrics[name].Set(labels, metric.Value)
}

func (e *exporter) stateLen() (int) {
	e.RLock()
	defer e.RUnlock()
	return len(e.Metrics)
}

func (e *exporter) readConfig() (conf config, err error) {
	log.Printf("reading config %s", e.configFile)
	bytes, err := ioutil.ReadFile(e.configFile)
	if err != nil {
		return
	}

	err = json.Unmarshal(bytes, &conf)
	return
}

func (e *exporter) reloadConfig() {
	conf, err := e.readConfig()
	if err != nil {
		log.Printf("Can't reload config: %s", err)
	} else {
		e.configChan <- conf
	}
}

func (e exporter) serveStatus() {
	http.Handle(e.metricsEndpoint, registry.DefaultHandler)
	http.ListenAndServe(e.listeningAddress, nil)
}

func New(configFile string) (e exporter, err error) {

	e = exporter{
		configFile:            configFile,
		Metrics:               make(map[string]metrics.Gauge),
		scrapeDuration:        metrics.NewDefaultHistogram(),
		metricsUpdated:        metrics.NewGauge(),
		metricsExported:       metrics.NewGauge(),
		configChan:            make(chan config),
		listeningAddress:      ":8080",
		metricsEndpoint:       "/metrics.json",
		gangliaScrapeInterval: 60 * time.Second,
	}

	conf, err := e.readConfig()
	if err != nil {
		return e, fmt.Errorf("Couldn't read config: %s", err)
	}
	e.conf = conf

	if conf.ListeningAddress != "" {
		e.listeningAddress = conf.ListeningAddress
	}
	if conf.MetricsEndpoint != "" {
		e.metricsEndpoint = conf.MetricsEndpoint
	}
	if conf.GangliaScrapeInterval != 0 {
		e.gangliaScrapeInterval = time.Duration(conf.GangliaScrapeInterval) * time.Second
	}

	registry.Register("gmond_exporter_scrape_duration_seconds", "gmond_exporter: Duration of a scrape job.", registry.NilLabels, e.scrapeDuration)
	registry.Register("gmond_exporter_metrics_updated_count", "gmond_exporter: Number of metrics updated.", registry.NilLabels, e.metricsUpdated)
	registry.Register("gmond_exporter_metrics_exported_count", "gmond_exporter: Number of metrics exported.", registry.NilLabels, e.metricsExported)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	go func() {
		for _ = range sig {
			e.reloadConfig() // sends a new config to configChan
		}
	}()

	go e.serveStatus()
	return e, nil
}

func (e *exporter) Loop() {
	ticker := time.Tick(e.gangliaScrapeInterval)
	for {
		select {
			case e.conf = <-e.configChan:
				log.Printf("Got new config")
			case <-ticker:
				log.Printf("Starting new scrape interval")
				e.scrapeAll()
				time.Sleep(e.gangliaScrapeInterval)
		}
	}
}

func (e *exporter) scrapeAll() {
	done := make(chan bool, len(e.conf.Endpoints))
	for _, addr := range e.conf.Endpoints {

		go func(addr string) {
			begin := time.Now()
			updates, err := e.fetchMetrics(addr)
			duration := time.Since(begin)

			endpointLabel := map[string]string{"endpoint": addr}
			durationLabel := map[string]string{"endpoint": addr}

			if err != nil {
				log.Printf("ERROR (after %fs): scraping %s: %s", duration.Seconds(), addr, err)
				durationLabel = map[string]string{"result": "error"}
			} else {
				e.metricsUpdated.Set(endpointLabel, float64(updates))
				e.metricsExported.Set(map[string]string{}, float64(e.stateLen()))
				log.Printf("OK (after %fs): %d metrics registered, %d values updated", duration.Seconds(), e.stateLen(), updates)
				durationLabel = map[string]string{"result": "success"}
			}
			e.scrapeDuration.Add(durationLabel, duration.Seconds())
			done <- true
		}(addr)
	}
	for i := 0; i < len(e.conf.Endpoints); i++ {
		<-done
	}
}

func (e *exporter) fetchMetrics(gangliaAddress string) (updates int, err error) {
	log.Printf("Scraping %s", gangliaAddress)
	conn, err := net.Dial(proto, gangliaAddress)
	if err != nil {
		return updates, fmt.Errorf("Can't connect to gmond: %s", err)
	}
	conn.SetDeadline(time.Now().Add(e.gangliaScrapeInterval))

	ganglia := Ganglia{}
	decoder := xml.NewDecoder(bufio.NewReader(conn))
	decoder.CharsetReader = toUtf8

	err = decoder.Decode(&ganglia)
	if err != nil {
		return updates, fmt.Errorf("Couldn't parse xml: %s", err)
	}

	for _, cluster := range ganglia.Clusters {
		for _, host := range cluster.Hosts {

			for _, metric := range host.Metrics {
				name := strings.ToLower(metric.Name)

				var labels = map[string]string{
					"hostname": host.Name,
					"cluster":  cluster.Name,
				}
				e.setMetric(name, labels, metric)
				updates++
			}
		}
	}
	return
}
