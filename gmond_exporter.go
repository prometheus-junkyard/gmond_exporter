package main

import (
	"bufio"
	"encoding/xml"
	"flag"
	"github.com/prometheus/client_golang"
	"github.com/prometheus/client_golang/metrics"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	proto = "tcp"
)

var (
	verbose               = flag.Bool("verbose", false, "Verbose output.")
	listeningAddress      = flag.String("listeningAddress", ":8080", "Address on which to expose JSON metrics.")
	metricsEndpoint       = flag.String("metricsEndpoint", "/metrics.json", "Path under which to expose JSON metrics.")
	gangliaAddress        = flag.String("gangliaAddress", "ganglia:8649", "gmond address.")
	gangliaScrapeInterval = flag.Int("gangliaScrapeInterval", 60, "Interval in seconds between scrapes.")
)

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

var gaugePerGangliaMetrics map[string]metrics.Gauge

func init() {
	gaugePerGangliaMetrics = make(map[string]metrics.Gauge)
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

func fetchMetrics(gmond_reader io.Reader) (updates int) {
	ganglia := Ganglia{}
	decoder := xml.NewDecoder(gmond_reader)
	decoder.CharsetReader = toUtf8

	err := decoder.Decode(&ganglia)
	if err != nil {
		log.Fatalf("Error: Couldn't parse xml: %s", err)
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
	return updates
}

func main() {
	flag.Parse()
	go serveStatus()
	func() {
		for {
			log.Printf("Scraping %s", *gangliaAddress)
			conn, err := net.Dial(proto, *gangliaAddress)
			if err != nil {
				log.Fatalf("Can't connect to gmond: %s", err)
			}
			updates := fetchMetrics(bufio.NewReader(conn))
			log.Printf("%d metrics registered, %d values updated", len(gaugePerGangliaMetrics), updates)
			time.Sleep(time.Duration(*gangliaScrapeInterval) * time.Second)
		}
	}()
}
