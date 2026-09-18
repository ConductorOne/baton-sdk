package pebble

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type reportPrototypeInterval struct {
	LowerMs, UpperMs uint64
	Available        bool
}

type reportPrototypeHistogram struct {
	buckets [65]uint64
	count   uint64
}

func (h *reportPrototypeHistogram) add(ms uint64) {
	h.buckets[bits.Len64(ms)]++
	h.count++
}

func (h reportPrototypeHistogram) quantile(percent uint64) reportPrototypeInterval {
	if h.count == 0 {
		return reportPrototypeInterval{}
	}
	rank := (h.count/100)*percent + (h.count%100*percent+99)/100
	var seen uint64
	for bucket, count := range h.buckets {
		seen += count
		if seen < rank {
			continue
		}
		out := reportPrototypeInterval{Available: true}
		if bucket > 0 {
			out.LowerMs = uint64(1) << (bucket - 1)
			out.UpperMs = math.MaxUint64
			if bucket < 64 {
				out.UpperMs = (uint64(1) << bucket) - 1
			}
		}
		return out
	}
	panic("invalid report histogram")
}

func reportPrototypeRankBefore(a, b reportPrototypeCollection) bool {
	if a.ConnectorMs != b.ConnectorMs {
		return a.ConnectorMs > b.ConnectorMs
	}
	x, y := a.Scope, b.Scope
	order := cmp.Or(cmp.Compare(x.Op, y.Op), cmp.Compare(x.ResourceTypeID, y.ResourceTypeID),
		cmp.Compare(x.ResourceID, y.ResourceID), cmp.Compare(x.ParentResourceTypeID, y.ParentResourceTypeID),
		cmp.Compare(x.ParentResourceID, y.ParentResourceID))
	if order != 0 {
		return order < 0
	}
	return !x.TypeScoped && y.TypeScoped
}

func reportPrototypeShare(part, total uint64) string {
	if total == 0 {
		return "unavailable"
	}
	return fmt.Sprintf("%.2f%%", 100*float64(part)/float64(total))
}

func renderReportPrototype(report reportPrototypeSummary) ([]byte, error) {
	functions := template.FuncMap{
		"share": reportPrototypeShare,
		"rate": func(v *float64) string {
			if v == nil {
				return "unavailable (zero writes)"
			}
			return fmt.Sprintf("%.2f", *v)
		},
		"interval": func(v reportPrototypeInterval) string {
			if !v.Available {
				return "unavailable"
			}
			if v.LowerMs == v.UpperMs {
				return fmt.Sprintf("%d", v.LowerMs)
			}
			return fmt.Sprintf("%d–%d", v.LowerMs, v.UpperMs)
		},
	}
	page, err := template.New("report").Funcs(functions).Parse(reportPrototypeHTML)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	err = page.Execute(&out, report)
	return out.Bytes(), err
}

func exportReportPrototype(report reportPrototypeSummary) error {
	dir := os.Getenv("LEDGER_REPORT_OUTPUT_DIR")
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	html, err := renderReportPrototype(report)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.html"), html, 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "report.json"), data, 0o600)
}

const reportPrototypeHTML = `<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ledger collection report · synthetic fixture</title>
<style>
body{font:16px/1.55 system-ui,sans-serif;
color:#193047;
background:#f5f7fa;
margin:0}main{max-width:1400px;
margin:auto;
padding:36px}
h1,h2{line-height:1.2}h1{font-size:32px}h2{margin-top:36px;
font-size:22px}p{max-width:1000px}.note{background:#fff1cc;
padding:16px;
border-radius:8px}
.scroll{overflow-x:auto;
background:white;
border:1px solid #dce2e9;
border-radius:8px}table{width:100%;
border-collapse:collapse;
font-size:14px}
th,td{padding:12px;
text-align:left;
border-bottom:1px solid #e3e7eb;
vertical-align:top}th{background:#eaf0f6;
white-space:normal}small{display:block;
color:#536578}code{font-size:13px}.metric{font-size:19px}
</style>
<main>
<h1>Ledger collection report</h1>
<p class="note">
<strong>Synthetic fixture, mechanically generated.</strong> Values are read from fixture pages committed through Pebble. Timing values are supplied by the fixture, not
measured connector performance. No AI-generated verdicts.</p>
<h2>Scope and recorded work</h2>
<p>Grants request: <strong>{{.GrantsRequest}}</strong>. Other effective options are unavailable in this prototype.</p>
<p class="metric">{{.Pages}} recorded pages · {{.Collections}} collection scopes · {{.Written}} record writes</p>
<p>{{.ReferenceChecks}} recorded references checked;
 {{.MissingContinuations}} missing continuations;
 {{.MissingChildren}} missing child references. Missing references are not proof of a connector bug: the file may be interrupted.</p>
<h2>Largest recorded connector-time consumers</h2>
<p>Top ten collection scopes, ranked by cumulative connector milliseconds. Share uses all scanned pages, including collections outside the top ten. Full identity
distinguishes parent scope and type-scoped collection.</p>
<p>Total recorded connector time: <strong>{{.ConnectorMs}} ms</strong>. Reported rate-limit waiting: <strong>{{.ReportedWaitMs}} ms</strong>. Waiting is included in
connector time, not added to it. Parallel worker time is not elapsed sync time.</p>
<div class="scroll">
<table>
<thead>
<tr>
<th>Collection scope</th>
<th>Pages / writes</th>
<th>Connector ms / share</th>
<th>Reported wait ms</th>
<th>Page median / p95 / max ms</th>
<th>Writes per page</th>
<th>Pages per 1,000 writes</th>
<th>Connector ms per 1,000 writes</th>
</tr>
</thead>
<tbody>
{{range .Top}}<tr>
<td>{{.Scope.Op}} · {{.Scope.ResourceTypeID}} / {{.Scope.ResourceID}}<small>Parent: {{.Scope.ParentResourceTypeID}} / {{.Scope.ParentResourceID}};
 type-scoped: {{.Scope.TypeScoped}}</small>
</td>
<td>{{.Pages}} / {{.Written}}<small>{{.ZeroWritePages}} zero-write pages</small>
</td>
<td>{{.ConnectorMs}} / {{share .ConnectorMs $.ConnectorMs}}</td>
<td>{{.ReportedWaitMs}}</td>
<td>{{interval .ConnectorPageMedian}} / {{interval .ConnectorPageP95}} / {{.MaxConnectorMs}}</td>
<td>{{printf "%.2f" .WrittenPerPage}}</td>
<td>{{rate .PagesPerThousandWrites}}</td>
<td>{{rate .ConnectorMsPerThousandWrites}}</td>
</tr>{{end}}
</tbody>
</table>
</div>
<p>Median and p95 are nearest-rank histogram intervals, in milliseconds;
 maximum is exact. Each observation is the recorded connector time for a page, not necessarily one API call. Zero milliseconds may reflect missing instrumentation. Writes
may include repeated identities and are not distinct final records. Rates for zero writes are undefined.</p>
<h2>Evidence for those collection scopes</h2>
<div class="scroll">
<table>
<thead>
<tr>
<th>Collection</th>
<th>Terminal pages</th>
<th>Missing continuations</th>
<th>Missing children</th>
<th>Recorded evidence</th>
</tr>
</thead>
<tbody>
{{range .Top}}<tr>
<td>{{.Scope.ResourceTypeID}} / {{.Scope.ResourceID}}<small>{{.Scope.Op}};
 parent {{.Scope.ParentResourceTypeID}} / {{.Scope.ParentResourceID}};
 type-scoped {{.Scope.TypeScoped}}</small>
</td>
<td>{{.TerminalPages}}</td>
<td>{{.MissingContinuations}}</td>
<td>{{.MissingChildren}}</td>
<td>{{.Outcome}}</td>
</tr>{{end}}
</tbody>
</table>
</div>
<p>Zero writes do not establish an empty endpoint: tolerated warnings, filtering and internal planning are not distinguished by the current row format. Resolved
references do not prove source completeness, root reachability or absence of cycles. No error reason is inferred from an absent row.</p>
<h2>Unavailable evidence</h2>
<p>Complete request arguments;
 per-page success versus warning;
 per-resource skip reasons;
 response counts before filtering;
 failed-attempt latency;
 retry breakdown by collection;
 exact wall-clock attribution. No conclusions about these are generated.</p>
<p>Generated by the Go ledger report prototype from stored rows and saved facts. No page tokens or raw error messages are included.</p>
</main>
</html>`

func TestLedgerReportHistogram(t *testing.T) {
	var h reportPrototypeHistogram
	require.False(t, h.quantile(50).Available)
	for _, ms := range []uint64{0, 1, 2, 3, 4, 200, 4000, math.MaxUint64} {
		h.add(ms)
	}
	require.Equal(t, reportPrototypeInterval{2, 3, true}, h.quantile(50))
	require.Equal(t, reportPrototypeInterval{uint64(1) << 63, math.MaxUint64, true}, h.quantile(95))
	var pair reportPrototypeHistogram
	pair.add(200)
	pair.add(4000)
	require.Equal(t, reportPrototypeInterval{128, 255, true}, pair.quantile(50))
	require.Equal(t, reportPrototypeInterval{2048, 4095, true}, pair.quantile(95))
}

func TestLedgerReportRendering(t *testing.T) {
	report := reportPrototypeSummary{ConnectorMs: 1000, Collections: 20}
	c := reportPrototypeCollection{ConnectorMs: 250}
	c.Scope.ResourceID = "<script>alert('bad')</script>"
	c.Scope.PageToken = "TOKEN-MUST-NOT-BE-EXPORTED"
	report.Top = []reportPrototypeCollection{c}
	first, err := renderReportPrototype(report)
	require.NoError(t, err)
	second, err := renderReportPrototype(report)
	require.NoError(t, err)
	require.Equal(t, first, second)
	output := string(first)
	require.Contains(t, output, "25.00%")
	require.Contains(t, output, "&lt;script&gt;")
	require.NotContains(t, output, "<script>")
	require.NotContains(t, output, c.Scope.PageToken)
	require.Contains(t, output, "unavailable (zero writes)")
	require.Equal(t, "unavailable", reportPrototypeShare(0, 0))
	require.False(t, strings.Contains(output, "NaN"))
}

func TestLedgerReportRank(t *testing.T) {
	a := reportPrototypeCollection{ConnectorMs: 100}
	b := reportPrototypeCollection{ConnectorMs: 200}
	a.Scope.ResourceID = "a"
	b.Scope.ResourceID = "b"
	require.True(t, reportPrototypeRankBefore(b, a))
	b.ConnectorMs = 100
	require.True(t, reportPrototypeRankBefore(a, b))
	require.False(t, reportPrototypeRankBefore(b, a))
	b.Scope = a.Scope
	b.Scope.TypeScoped = true
	require.True(t, reportPrototypeRankBefore(a, b))
	require.False(t, reportPrototypeRankBefore(a, a))
}
