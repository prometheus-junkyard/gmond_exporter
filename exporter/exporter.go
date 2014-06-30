package exporter

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	proto = "tcp"

	namespace = "gmond_exporter"
)

var (
	verbose = flag.Bool("verbose", false, "Verbose output.")

	prometheusLabels = []string{"hostname", "cluster"}
)

// For unmarshaling config file.
type config struct {
	Endpoints             []string `json:"endpoints"`
	ListeningAddress      string   `json:"listeningAddress"`
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
	Metrics               map[string]*prometheus.GaugeVec
	configFile            string
	listeningAddress      string
	gangliaScrapeInterval time.Duration

	verbose    bool
	conf       config
	configChan chan config

	scrapeDuration  *prometheus.SummaryVec
	metricsUpdated  *prometheus.GaugeVec
	metricsExported prometheus.Gauge
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
		gv := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: name,
				Help: desc,
			},
			prometheusLabels,
		)
		e.Metrics[name] = gv
		prometheus.Register(gv) // One GaugeVec per metric!
	}
	e.Metrics[name].With(labels).Set(metric.Value)
}

func (e *exporter) stateLen() int {
	e.RLock()
	defer e.RUnlock()
	return len(e.Metrics)
}

func (e *exporter) readConfig() (conf config, err error) {
	log.Printf("Reading config %s", e.configFile)
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
	http.Handle("/metrics", prometheus.Handler())
	http.ListenAndServe(e.listeningAddress, nil)
}

func New(configFile string) (e exporter, err error) {

	e = exporter{
		configFile: configFile,
		Metrics:    map[string]*prometheus.GaugeVec{},
		scrapeDuration: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: namespace,
				Name:      "scrape_duration_seconds",
				Help:      "gmond_exporter: Duration of a scrape job.",
			},
			[]string{"endpoint", "result"},
		),
		metricsUpdated: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "metrics_updated_count",
				Help:      "gmond_exporter: Number of metrics updated.",
			},
			[]string{"endpoint"},
		),
		metricsExported: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "metrics_exported_count",
			Help:      "gmond_exporter: Number of metrics exported.",
		}),
		configChan:            make(chan config),
		listeningAddress:      ":8080",
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
	if conf.GangliaScrapeInterval != 0 {
		e.gangliaScrapeInterval = time.Duration(conf.GangliaScrapeInterval) * time.Second
	}

	prometheus.MustRegister(e.scrapeDuration)
	prometheus.MustRegister(e.metricsUpdated)
	prometheus.MustRegister(e.metricsExported)
	debug("Registered internal metrics")

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
	debug("Loop initialized, running every %s", e.gangliaScrapeInterval)
	ticker := time.Tick(e.gangliaScrapeInterval)
	for {
		select {
		case e.conf = <-e.configChan:
			log.Printf("Got new config")
		case <-ticker:
			log.Printf("Starting new scrape interval")
			e.scrapeAll()
		}
	}
}

func (e *exporter) scrapeAll() {
	wg := sync.WaitGroup{}
	wg.Add(len(e.conf.Endpoints))
	for _, addr := range e.conf.Endpoints {

		go func(addr string) {
			begin := time.Now()
			updates, err := e.fetchMetrics(addr)
			duration := time.Since(begin)

			durationLabels := prometheus.Labels{"endpoint": addr}

			if err != nil {
				log.Printf("ERROR (after %fs): scraping %s: %s", duration.Seconds(), addr, err)
				durationLabels["result"] = "error"
			} else {
				e.metricsUpdated.WithLabelValues(addr).Set(float64(updates))
				e.metricsExported.Set(float64(e.stateLen()))
				log.Printf("OK (after %fs): %d metrics registered, %d values updated", duration.Seconds(), e.stateLen(), updates)
				durationLabels["result"] = "success"
			}
			e.scrapeDuration.With(durationLabels).Observe(duration.Seconds())
			wg.Done()
		}(addr)
	}
	wg.Wait()
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
