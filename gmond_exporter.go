package main

import (
	"bufio"
	"encoding/json"
	"encoding/xml"
	"flag"
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
	"syscall"
	"time"
)

const (
	proto = "tcp"
)

var (
	conf                   config
	verbose                = flag.Bool("verbose", false, "Verbose output.")
	listeningAddress       = flag.String("listeningAddress", ":8080", "Address on which to expose JSON metrics.")
	metricsEndpoint        = flag.String("metricsEndpoint", "/metrics.json", "Path under which to expose JSON metrics.")
	configFile             = flag.String("config", "gmond_exporter.conf", "config file.")
	gangliaScrapeDelay     = flag.Int("gangliaScrapeDelay", 60, "Delay in seconds between scrapes. Abort scrapes taking longer than that.")
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

func init() {
	flag.Parse()
	gaugePerGangliaMetrics = make(map[string]metrics.Gauge)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	go func() {
		for _ = range sig {
			err := readConfig()
			if err != nil {
				log.Printf("Couldn't reload config: %s", err)
			}
		}
	}()

	err := readConfig()
	if err != nil {
		log.Fatalf("Couldn't read config: %s", err)
	}

}

func readConfig() (err error) {
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
		log.Println("Can't connect to gmond")
		return
	}
	conn.SetDeadline(time.Now().Add(time.Duration(*gangliaScrapeDelay * int(time.Second))))

	ganglia := Ganglia{}
	decoder := xml.NewDecoder(bufio.NewReader(conn))
	decoder.CharsetReader = toUtf8

	err = decoder.Decode(&ganglia)
	if err != nil {
		log.Println("Couldn't parse xml")
		return
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
	scrapeDuration := metrics.NewGauge()
	metricsUpdated := metrics.NewGauge()
	metricsExported := metrics.NewGauge()
	registry.DefaultRegistry.Register("gmond_exporter_scape_duration_seconds", "gmond_exporter: Duration of a scape job.", registry.NilLabels, scrapeDuration)
	registry.DefaultRegistry.Register("gmond_exporter_metrics_updated_count", "gmond_exporter: Number of metrics updated.", registry.NilLabels, metricsUpdated)
	registry.DefaultRegistry.Register("gmond_exporter_metrics_exported_count", "gmond_exporter: Number of metrics exported.", registry.NilLabels, metricsExported)

	go serveStatus()

	for {
		done := make(chan bool, len(conf.Endpoints))
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
				scrapeDuration.Set(durationLabel, duration.Seconds())
				done <- true
			}(addr)
		}
		for i := 0; i < len(conf.Endpoints); i++ {
			<-done
		}

		time.Sleep(time.Duration(*gangliaScrapeDelay) * time.Second)
	}
}
