package main

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"github.com/prometheus/client_golang"
	"github.com/prometheus/client_golang/maths"
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

var (
	verbose                = flag.Bool("verbose", false, "Verbose output.")
	listeningAddress       = flag.String("listeningAddress", ":8080", "Address on which to expose JSON metrics.")
	metricsEndpoint        = flag.String("metricsEndpoint", "/metrics.json", "Path under which to expose JSON metrics.")
	configFile             = flag.String("config", "gmond_exporter.conf", "config file.")
	gangliaScrapeInterval  = flag.Duration("gangliaScrapeInterval", 1*time.Minute, "Interval between scrapes.")
	gaugePerGangliaMetrics map[string]metrics.Gauge
)

type config struct {
	Endpoints []string `json:"endpoints"`
}

type ExtraElement struct {
	Name string `xml:"NAME,attr"`
	Val  string `xml:"VAL,attr"`
}

type ExtraData struct {
	ExtraElements []ExtraElement `xml:"EXTRA_ELEMENT"`
}

type Metric struct {
	Name      string    `xml:"NAME,attr"`
	Value     float64   `xml:"VAL,attr"`
	Unit      string    `xml:"UNITS,attr"`
	Slope     string    `xml:"SLOPE,attr"`
	Tn        int       `xml:"TN,attr"`
	Tmax      int       `xml:"TMAX,attr"`
	Dmax      int       `xml:"DMAX,attr"`
	ExtraData ExtraData `xml:"EXTRA_DATA"`
}

type Host struct {
	Name         string `xml:"NAME,attr"`
	Ip           string `xml:"IP,attr"`
	Tags         string `xml:"TAGS,attr"`
	Reported     int    `xml:"REPORTED,attr"`
	Tn           int    `xml:"TN,attr"`
	Tmax         int    `xml:"TMAX,attr"`
	Dmax         int    `xml:"DMAX,attr"`
	Location     string `xml:"LOCATION,attr"`
	GmondStarted int    `xml:"GMOND_STARTED",attr"`

	Metrics []Metric `xml:"METRIC"`
}

type Cluster struct {
	Name      string `xml:"NAME,attr"`
	Owner     string `xml:"OWNER,attr"`
	LatLong   string `xml:"LATLONG,attr"`
	Url       string `xml:"URL,attr"`
	Localtime int    `xml:"LOCALTIME,attr"`
	Hosts     []Host `xml:"HOST"`
}

type Ganglia struct {
	XMLNAME  xml.Name  `xml:"GANGLIA_XML"`
	Clusters []Cluster `xml:"CLUSTER"`
}

func readConfig() (conf config, err error) {
	log.Printf("reading config %s", *configFile)
	bytes, err := ioutil.ReadFile(*configFile)
	if err != nil {
		return
	}

	err = json.Unmarshal(bytes, &conf)
	return
}

func debug(format string, a ...interface{}) {
	if *verbose {
		log.Printf(format, a...)
	}
}

func serveStatus() {
	exporter := registry.DefaultRegistry.Handler()

	http.Handle(*metricsEndpoint, exporter)
	http.ListenAndServe(*listeningAddress, nil)
}

func toUtf8(charset string, input io.Reader) (io.Reader, error) {
	return input, nil //FIXME
}

func fetchMetrics(gangliaAddress string) (updates int, err error) {
	log.Printf("Scraping %s", gangliaAddress)
	conn, err := net.Dial(proto, gangliaAddress)
	if err != nil {
		return updates, fmt.Errorf("Can't connect to gmond: %s", err)
	}
	conn.SetDeadline(time.Now().Add(time.Duration(*gangliaScrapeInterval) * time.Second))

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
				if _, ok := gaugePerGangliaMetrics[name]; !ok {
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
					gaugePerGangliaMetrics[name] = gauge
					registry.DefaultRegistry.Register(name, desc, registry.NilLabels, gauge) // one gauge per metric!
				}

				var labels = map[string]string{
					"hostname": host.Name,
					"cluster":  cluster.Name,
				}
				debug("%s{%s} = %f", name, labels, metric.Value)
				gaugePerGangliaMetrics[name].Set(labels, metric.Value)
				updates = updates + 1
			}
		}
	}
	return
}

func main() {
	flag.Parse()
	gaugePerGangliaMetrics = make(map[string]metrics.Gauge)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	configChan := make(chan config)
	go func() {
		for _ = range sig {
			config, err := readConfig()
			if err != nil {
				log.Printf("Couldn't reload config: %s", err)
				continue
			}
			configChan <- config
		}
	}()

	DurationSpecification := &metrics.HistogramSpecification{
		Starts:                metrics.LogarithmicSizedBucketsFor(0, 1000),
		BucketBuilder:         metrics.AccumulatingBucketBuilder(metrics.EvictAndReplaceWith(10, maths.Average), 100),
		ReportablePercentiles: []float64{0.01, 0.05, 0.5, 0.90, 0.99},
	}

	scrapeDuration := metrics.NewHistogram(DurationSpecification)
	metricsUpdated := metrics.NewGauge()
	metricsExported := metrics.NewGauge()
	registry.DefaultRegistry.Register("gmond_exporter_scrape_duration_seconds", "gmond_exporter: Duration of a scrape job.", registry.NilLabels, scrapeDuration)
	registry.DefaultRegistry.Register("gmond_exporter_metrics_updated_count", "gmond_exporter: Number of metrics updated.", registry.NilLabels, metricsUpdated)
	registry.DefaultRegistry.Register("gmond_exporter_metrics_exported_count", "gmond_exporter: Number of metrics exported.", registry.NilLabels, metricsExported)

	conf, err := readConfig()
	if err != nil {
		log.Fatalf("Couldn't read config: %s", err)
	}

	go serveStatus()

	for {
		log.Printf("Starting new scrape interval")
		select {
		case conf = <-configChan:
			log.Printf("Got new config")
		default:
			wg := &sync.WaitGroup{}
			wg.Add(len(conf.Endpoints))
			for _, addr := range conf.Endpoints {
				go func(addr string) {
					begin := time.Now()
					updates, err := fetchMetrics(addr)
					duration := time.Since(begin)

					endpointLabel := map[string]string{"endpoint": addr}
					durationLabel := map[string]string{"endpoint": addr}

					if err != nil {
						log.Printf("ERROR (after %fs): scraping %s: %s", duration.Seconds(), addr, err)
						durationLabel = map[string]string{"result": "error"}
					} else {
						metricsUpdated.Set(endpointLabel, float64(updates))
						metricsExported.Set(map[string]string{}, float64(len(gaugePerGangliaMetrics)))
						log.Printf("OK (after %fs): %d metrics registered, %d values updated", duration.Seconds(), len(gaugePerGangliaMetrics), updates)
						durationLabel = map[string]string{"result": "success"}
					}
					scrapeDuration.Add(durationLabel, duration.Seconds())
					wg.Done()
				}(addr)
			}
			wg.Wait()
			time.Sleep(*gangliaScrapeInterval)
		}
	}
}
