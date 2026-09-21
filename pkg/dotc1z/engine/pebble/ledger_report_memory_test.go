package pebble

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type reportPrototypeMemory struct {
	heapStart, heapPeak, rssStart, rssPeak uint64
	samples                                uint64
}

func sampleReportPrototypeMemory() *reportPrototypeMemory {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	out := &reportPrototypeMemory{heapStart: m.HeapAlloc, heapPeak: m.HeapAlloc, samples: 1}
	data, err := os.ReadFile("/proc/self/status")
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 3 && fields[0] == "VmRSS:" {
				n, err := strconv.ParseUint(fields[1], 10, 64)
				if err == nil {
					out.rssStart = n * 1024
					out.rssPeak = n * 1024
				}
			}
		}
	}
	return out
}

func startReportPrototypeMemory(b *testing.B) func() {
	b.Helper()
	if os.Getenv("LEDGER_REPORT_MEMORY") != "1" {
		return func() {}
	}
	runtime.GC()
	peak := sampleReportPrototypeMemory()
	stop := make(chan struct{})
	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				m := sampleReportPrototypeMemory()
				peak.heapPeak = max(peak.heapPeak, m.heapPeak)
				peak.rssPeak = max(peak.rssPeak, m.rssPeak)
				peak.samples++
			}
		}
	}()
	return func() {
		close(stop)
		done.Wait()
		end := sampleReportPrototypeMemory()
		peak.heapPeak = max(peak.heapPeak, end.heapPeak)
		peak.rssPeak = max(peak.rssPeak, end.rssPeak)
		runtime.GC()
		afterGC := sampleReportPrototypeMemory()
		b.ReportMetric(float64(peak.heapStart), "heap-start-B")
		b.ReportMetric(float64(peak.heapPeak), "heap-sampled-peak-B")
		b.ReportMetric(float64(afterGC.heapStart), "heap-after-gc-B")
		b.ReportMetric(float64(peak.rssStart), "rss-start-B")
		b.ReportMetric(float64(peak.rssPeak), "rss-sampled-peak-B")
		b.ReportMetric(float64(peak.samples), "memory-samples")
	}
}
