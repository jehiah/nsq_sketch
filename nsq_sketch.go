// give a top-n view of data in a NSQ stream of json messages

package main

import (
	"bitly/nsq"
	"flag"
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/jehiah/countmin"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

const VERSION = "0.1"

var (
	sketchKey             = flag.String("key", "", "key to extract from message and sketch")
	sketchDisplayInterval = flag.String("interval", "5s", "duration to wait before clearing the sketch")
	sketchResetInterval   = flag.String("reset-every", "120s", "duration to wait before resetting the sketch")
	topN                  = flag.Int("top", 15, "show the top N entries")
	sketchWidth           = flag.Int("width", 7, "Number of sketch hash functions to use")
	sketchHeight          = flag.Int("height", 20000, "Number of elements in each sketch array")

	topic            = flag.String("topic", "", "nsq topic")
	channel          = flag.String("channel", "sketch#ephemeral", "nsq channel")
	verbose          = flag.Bool("verbose", false, "verbose logging")
	maxInFlight      = flag.Int("max-in-flight", 1000, "max number of messages to allow in flight")
	showVersion      = flag.Bool("version", false, "print version string")
	nsqdTCPAddrs     = StringArray{}
	lookupdHTTPAddrs = StringArray{}
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type StringArray []string

func (a *StringArray) Set(s string) error {
	*a = append(*a, s)
	return nil
}

func (a *StringArray) String() string {
	return fmt.Sprint(*a)
}

type HeavyHitter struct {
	Key   string
	Value uint32
}
type HeavyHitters []*HeavyHitter
type HeavyHittersByValue struct {
	HeavyHitters
}

func (h HeavyHitters) Len() int      { return len(h) }
func (h HeavyHitters) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h HeavyHittersByValue) Less(i, j int) bool {
	return h.HeavyHitters[i].Value > h.HeavyHitters[j].Value
}

type SketchHandler struct {
	sync.RWMutex
	Sketch       countmin.Sketch
	HeavyHitters HeavyHitters
	messageCount uint64
}

func (s *SketchHandler) HandleMessage(m *nsq.Message) error {
	data, err := simplejson.NewJson(m.Body)
	if err != nil {
		return err
	}

	if _, ok := data.CheckGet(*sketchKey); !ok {
		return nil
	}
	key, err := data.Get(*sketchKey).String()
	if err != nil {
		return nil // don't backoff for failed messages
	}
	s.Lock()
	defer s.Unlock()
	s.messageCount += 1
	count := s.Sketch.AddString(key, 1)
	var minPos int
	var minValue uint32
	for i, h := range s.HeavyHitters {
		if h.Key == key {
			h.Value = count
			return nil
		}
		if i == 0 {
			minValue = h.Value
		} else {
			if h.Value < minValue {
				minPos = i
				minValue = h.Value
			}
		}
	}
	h := &HeavyHitter{key, count}
	if len(s.HeavyHitters) < *topN {
		s.HeavyHitters = append(s.HeavyHitters, h)
	} else if count > minValue {
		s.HeavyHitters[minPos] = h
	}
	return nil
}

func (s *SketchHandler) TimerLoop(d time.Duration, r time.Duration) {
	displayTicker := time.NewTicker(d)
	resetTicker := time.NewTicker(r)
	for {
		select {
		case <-resetTicker.C:
			s.Lock()
			fmt.Fprintf(os.Stdout, "\n-[reset sketch. next reset in %s]-\n", r)
			s.Sketch = countmin.NewCountMinSketch(7, 20000)
			s.HeavyHitters = make([]*HeavyHitter, 0)
			s.messageCount = 0
			s.Unlock()
		case <-displayTicker.C:
			fmt.Fprintf(os.Stdout, "\n----- %d messages ----\n", s.messageCount)
			s.Lock()
			sort.Sort(HeavyHittersByValue{s.HeavyHitters})
			for _, h := range s.HeavyHitters {
				fmt.Fprintf(os.Stdout, "%d - %s\n", h.Value, h.Key)
			}
			s.Unlock()
		}
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_sketch v%s\n", VERSION)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
	}

	if *sketchKey == "" {
		log.Fatalf("--key required to speicfy which message element to sketch")
	}

	if *maxInFlight < 0 {
		log.Fatalf("--max-in-flight must be > 0")
	}

	if *topN < 1 {
		log.Fatal("--top must be > 0 to specify the number of items to track")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required.")
	}
	if len(nsqdTCPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-http-address not both")
	}
	displayInterval, err := time.ParseDuration(*sketchDisplayInterval)
	if err != nil {
		log.Fatalf("failed parsing --interval %s. %s", *sketchDisplayInterval, err.Error())
	}
	resetInterval, err := time.ParseDuration(*sketchResetInterval)
	if err != nil {
		log.Fatalf("failed parsing --reset-every %s. %s", *sketchResetInterval, err.Error())
	}

	log.Printf("Sketching for %s intervals. Displaying %d top items every %s", resetInterval, *topN, displayInterval)

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	s := &SketchHandler{
		Sketch:       countmin.NewCountMinSketch(*sketchWidth, *sketchHeight),
		HeavyHitters: make([]*HeavyHitter, 0),
	}
	if !*verbose {
		log.SetOutput(ioutil.Discard)
	}

	r, err := nsq.NewReader(*topic, *channel)
	if err != nil {
		log.Fatalf(err.Error())
	}
	r.SetMaxInFlight(*maxInFlight)
	r.VerboseLogging = *verbose

	r.AddHandler(s)

	go s.TimerLoop(displayInterval, resetInterval)

	go func() {
		select {
		case <-termChan:
			r.Stop()
		}
	}()

	for _, addrString := range nsqdTCPAddrs {
		err := r.ConnectToNSQ(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdHTTPAddrs {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	<-r.ExitChan
}
