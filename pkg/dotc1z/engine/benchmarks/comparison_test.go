package benchmarks

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/sqlite"
)

var (
	benchScale  = flag.String("bench-scale", "small", "Benchmark scale: small, medium, large")
	benchOutput = flag.String("bench-output", "engine_comparison_results.json", "Output file for detailed results")
)

// BenchmarkResult holds the results of a benchmark run
type BenchmarkResult struct {
	Name           string        `json:"name"`
	Engine         string        `json:"engine"`
	Duration       time.Duration `json:"duration"`
	Operations     int64         `json:"operations"`
	OpsPerSecond   float64       `json:"ops_per_second"`
	BytesAllocated uint64        `json:"bytes_allocated"`
	AllocsPerOp    float64       `json:"allocs_per_op"`
	TotalAllocs    uint64        `json:"total_allocs"`
	Error          string        `json:"error,omitempty"`
}

// ComparisonReport holds the complete comparison between engines
type ComparisonReport struct {
	Timestamp     time.Time         `json:"timestamp"`
	Environment   Environment       `json:"environment"`
	PebbleResults []BenchmarkResult `json:"pebble_results"`
	SQLiteResults []BenchmarkResult `json:"sqlite_results"`
	Comparisons   []Comparison      `json:"comparisons"`
	Summary       Summary           `json:"summary"`
}

// Environment captures the test environment details
type Environment struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	NumCPU       int    `json:"num_cpu"`
	GoVersion    string `json:"go_version"`
}

// Comparison holds the relative performance between engines for a specific test
type Comparison struct {
	TestName             string  `json:"test_name"`
	PebbleOpsPerSec      float64 `json:"pebble_ops_per_sec"`
	SQLiteOpsPerSec      float64 `json:"sqlite_ops_per_sec"`
	SpeedupRatio         float64 `json:"speedup_ratio"` // Positive means Pebble is faster, negative means SQLite is faster
	PebbleAllocsPerOp    float64 `json:"pebble_allocs_per_op"`
	SQLiteAllocsPerOp    float64 `json:"sqlite_allocs_per_op"`
	AllocRatio           float64 `json:"alloc_ratio"` // Positive means Pebble uses more allocs per op
	AllocPercentageDiff  float64 `json:"alloc_percentage_diff"` // Percentage difference in allocs per op
	Winner               string  `json:"winner"`
}

// Summary provides overall comparison statistics
type Summary struct {
	TotalTests           int     `json:"total_tests"`
	PebbleWins           int     `json:"pebble_wins"`
	SQLiteWins           int     `json:"sqlite_wins"`
	AverageSpeedupRatio  float64 `json:"average_speedup_ratio"`
	AverageAllocRatio    float64 `json:"average_alloc_ratio"`
	PebbleAvgOpsPerSec   float64 `json:"pebble_avg_ops_per_sec"`
	SQLiteAvgOpsPerSec   float64 `json:"sqlite_avg_ops_per_sec"`
	RecommendedEngine    string  `json:"recommended_engine"`
	RecommendationReason string  `json:"recommendation_reason"`
}

// BenchmarkSuite contains all the benchmark tests
type BenchmarkSuite struct {
	ctx    context.Context
	report *ComparisonReport
	mu     sync.Mutex
}

// NewBenchmarkSuite creates a new benchmark suite
func NewBenchmarkSuite(ctx context.Context) *BenchmarkSuite {
	return &BenchmarkSuite{
		ctx: ctx,
		report: &ComparisonReport{
			Timestamp: time.Now(),
			Environment: Environment{
				OS:           runtime.GOOS,
				Architecture: runtime.GOARCH,
				NumCPU:       runtime.NumCPU(),
				GoVersion:    runtime.Version(),
			},
		},
	}
}

// createPebbleEngine creates a new Pebble engine for testing
func (bs *BenchmarkSuite) createPebbleEngine(testName string) (engine.StorageEngine, func(), error) {
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("pebble_comparison_%s", testName))
	if err != nil {
		return nil, nil, err
	}

	pe, err := pebble.NewPebbleEngine(bs.ctx, tempDir,
		pebble.WithCacheSize(128<<20), // 128MB cache
		pebble.WithBatchSizeLimit(5000),
		pebble.WithSlowQueryThreshold(1*time.Second),
	)
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, nil, err
	}

	cleanup := func() {
		pe.Close()
		os.RemoveAll(tempDir)
	}

	return pe, cleanup, nil
}

// createSQLiteEngine creates a new SQLite engine for testing
func (bs *BenchmarkSuite) createSQLiteEngine(testName string) (engine.StorageEngine, func(), error) {
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("sqlite_comparison_%s", testName))
	if err != nil {
		return nil, nil, err
	}

	se, err := sqlite.NewSQLite(bs.ctx, tempDir, sqlite.WithPragma("journal_mode", "WAL"))
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, nil, err
	}

	cleanup := func() {
		se.Close()
		os.RemoveAll(tempDir)
	}

	return se, cleanup, nil
}

// runBenchmark runs a benchmark function and collects metrics
func (bs *BenchmarkSuite) runBenchmark(name, engineType string, benchFunc func(engine.StorageEngine) error, eng engine.StorageEngine) BenchmarkResult {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	start := time.Now()
	err := benchFunc(eng)
	duration := time.Since(start)

	runtime.ReadMemStats(&m2)

	result := BenchmarkResult{
		Name:           name,
		Engine:         engineType,
		Duration:       duration,
		BytesAllocated: m2.TotalAlloc - m1.TotalAlloc,
		TotalAllocs:    m2.Mallocs - m1.Mallocs,
	}

	if err != nil {
		result.Error = err.Error()
	}

	return result
}

// updateAllocsPerOp calculates allocs per operation after operations are set
func (result *BenchmarkResult) updateAllocsPerOp() {
	if result.Operations > 0 {
		result.AllocsPerOp = float64(result.TotalAllocs) / float64(result.Operations)
	}
}

// addResult safely adds a benchmark result to the report
func (bs *BenchmarkSuite) addResult(result BenchmarkResult) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if result.Engine == "pebble" {
		bs.report.PebbleResults = append(bs.report.PebbleResults, result)
	} else {
		bs.report.SQLiteResults = append(bs.report.SQLiteResults, result)
	}
}

// generateDatasets creates test data for benchmarks
func generateResources(count int, prefix string) []*v2.Resource {
	resources := make([]*v2.Resource, count)
	for i := 0; i < count; i++ {
		resources[i] = &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: fmt.Sprintf("%s-type-%d", prefix, i%100), // Mix of types
				Resource:     fmt.Sprintf("%s-resource-%d", prefix, i),
			},
			DisplayName: fmt.Sprintf("%s Resource %d", prefix, i),
			Description: fmt.Sprintf("Generated resource %d for performance testing with some additional text to make the payload more realistic", i),
		}
	}
	return resources
}

func generateGrants(count int, prefix string) []*v2.Grant {
	grants := make([]*v2.Grant, count)
	for i := 0; i < count; i++ {
		grants[i] = &v2.Grant{
			Id: fmt.Sprintf("%s-grant-%d", prefix, i),
			Entitlement: &v2.Entitlement{
				Id:          fmt.Sprintf("%s-entitlement-%d", prefix, i%1000), // Reuse entitlements
				DisplayName: fmt.Sprintf("%s Entitlement %d", prefix, i%1000),
			},
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     fmt.Sprintf("%s-user-%d", prefix, i%500), // Reuse principals
				},
				DisplayName: fmt.Sprintf("%s User %d", prefix, i%500),
			},
		}
	}
	return grants
}

// Benchmark tests start here
func (bs *BenchmarkSuite) BenchmarkRawInserts(count int) {
	testName := fmt.Sprintf("RawInserts_%d", count)

	// Test Pebble
	pe, pCleanup, err := bs.createPebbleEngine(testName + "_pebble")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "pebble",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer pCleanup()

		resources := generateResources(count, "insert")

		result := bs.runBenchmark(testName, "pebble", func(eng engine.StorageEngine) error {
			_, err := eng.StartNewSync(bs.ctx)
			if err != nil {
				return err
			}

			err = eng.PutResources(bs.ctx, resources...)
			if err != nil {
				return err
			}

			return eng.EndSync(bs.ctx)
		}, pe)

		result.Operations = int64(count)
		result.updateAllocsPerOp()
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}

	// Test SQLite
	se, sCleanup, err := bs.createSQLiteEngine(testName + "_sqlite")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "sqlite",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer sCleanup()

		resources := generateResources(count, "insert")

		result := bs.runBenchmark(testName, "sqlite", func(eng engine.StorageEngine) error {
			_, err := eng.StartNewSync(bs.ctx)
			if err != nil {
				return err
			}

			err = eng.PutResources(bs.ctx, resources...)
			if err != nil {
				return err
			}

			return eng.EndSync(bs.ctx)
		}, se)

		result.Operations = int64(count)
		result.updateAllocsPerOp()
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}
}

func (bs *BenchmarkSuite) BenchmarkSequentialReads(setupCount, readCount int) {
	testName := fmt.Sprintf("SequentialReads_setup_%d_read_%d", setupCount, readCount)

	// Test Pebble
	pe, pCleanup, err := bs.createPebbleEngine(testName + "_pebble")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "pebble",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer pCleanup()

		// Setup data
		resources := generateResources(setupCount, "seqread")
		_, err := pe.StartNewSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}
		err = pe.PutResources(bs.ctx, resources...)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}
		err = pe.EndSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}

		result := bs.runBenchmark(testName, "pebble", func(eng engine.StorageEngine) error {
			req := &v2.ResourcesServiceListResourcesRequest{
				PageSize: uint32(readCount),
			}

			_, err := eng.ListResources(bs.ctx, req)
			return err
		}, pe)

		result.Operations = int64(readCount)
		result.updateAllocsPerOp()
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}

	// Test SQLite
	se, sCleanup, err := bs.createSQLiteEngine(testName + "_sqlite")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "sqlite",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer sCleanup()

		// Setup data
		resources := generateResources(setupCount, "seqread")
		_, err := se.StartNewSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}
		err = se.PutResources(bs.ctx, resources...)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}
		err = se.EndSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}

		result := bs.runBenchmark(testName, "sqlite", func(eng engine.StorageEngine) error {
			req := &v2.ResourcesServiceListResourcesRequest{
				PageSize: uint32(readCount),
			}

			_, err := eng.ListResources(bs.ctx, req)
			return err
		}, se)

		result.Operations = int64(readCount)
		result.updateAllocsPerOp()
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}
}

func (bs *BenchmarkSuite) BenchmarkRandomReads(setupCount, readCount int) {
	testName := fmt.Sprintf("RandomReads_setup_%d_read_%d", setupCount, readCount)

	// Test Pebble
	pe, pCleanup, err := bs.createPebbleEngine(testName + "_pebble")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "pebble",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer pCleanup()

		// Setup data
		resources := generateResources(setupCount, "randread")
		_, err := pe.StartNewSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}
		err = pe.PutResources(bs.ctx, resources...)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}
		err = pe.EndSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}

		result := bs.runBenchmark(testName, "pebble", func(eng engine.StorageEngine) error {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < readCount; i++ {
				idx := r.Intn(setupCount)
				req := &reader_v2.ResourcesReaderServiceGetResourceRequest{
					ResourceId: &v2.ResourceId{
						ResourceType: fmt.Sprintf("randread-type-%d", idx%100),
						Resource:     fmt.Sprintf("randread-resource-%d", idx),
					},
				}

				_, err := eng.GetResource(bs.ctx, req)
				if err != nil {
					return err
				}
			}
			return nil
		}, pe)

		result.Operations = int64(readCount)
		result.updateAllocsPerOp()
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}

	// Test SQLite
	se, sCleanup, err := bs.createSQLiteEngine(testName + "_sqlite")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "sqlite",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer sCleanup()

		// Setup data
		resources := generateResources(setupCount, "randread")
		_, err := se.StartNewSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}
		err = se.PutResources(bs.ctx, resources...)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}
		err = se.EndSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}

		result := bs.runBenchmark(testName, "sqlite", func(eng engine.StorageEngine) error {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < readCount; i++ {
				idx := r.Intn(setupCount)
				req := &reader_v2.ResourcesReaderServiceGetResourceRequest{
					ResourceId: &v2.ResourceId{
						ResourceType: fmt.Sprintf("randread-type-%d", idx%100),
						Resource:     fmt.Sprintf("randread-resource-%d", idx),
					},
				}

				_, err := eng.GetResource(bs.ctx, req)
				if err != nil {
					return err
				}
			}
			return nil
		}, se)

		result.Operations = int64(readCount)
		result.updateAllocsPerOp()
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}
}

func (bs *BenchmarkSuite) BenchmarkGrantOperations(grantCount int) {
	testName := fmt.Sprintf("GrantOperations_%d", grantCount)

	// Test Pebble
	pe, pCleanup, err := bs.createPebbleEngine(testName + "_pebble")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "pebble",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer pCleanup()

		grants := generateGrants(grantCount, "grantops")

		result := bs.runBenchmark(testName, "pebble", func(eng engine.StorageEngine) error {
			_, err := eng.StartNewSync(bs.ctx)
			if err != nil {
				return err
			}

			err = eng.PutGrants(bs.ctx, grants...)
			if err != nil {
				return err
			}

			return eng.EndSync(bs.ctx)
		}, pe)

		result.Operations = int64(grantCount)
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}

	// Test SQLite
	se, sCleanup, err := bs.createSQLiteEngine(testName + "_sqlite")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "sqlite",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer sCleanup()

		grants := generateGrants(grantCount, "grantops")

		result := bs.runBenchmark(testName, "sqlite", func(eng engine.StorageEngine) error {
			_, err := eng.StartNewSync(bs.ctx)
			if err != nil {
				return err
			}

			err = eng.PutGrants(bs.ctx, grants...)
			if err != nil {
				return err
			}

			return eng.EndSync(bs.ctx)
		}, se)

		result.Operations = int64(grantCount)
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}
}

// BenchmarkUpsertIfNewer tests upsert performance on existing data
func (bs *BenchmarkSuite) BenchmarkUpsertIfNewer(setupCount, upsertCount int) {
	testName := fmt.Sprintf("UpsertIfNewer_setup_%d_upsert_%d", setupCount, upsertCount)

	// Test Pebble
	pe, pCleanup, err := bs.createPebbleEngine(testName + "_pebble")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "pebble",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer pCleanup()

		// Setup initial data (outside measurement)
		resources := generateResources(setupCount, "upsert")
		_, err := pe.StartNewSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}
		err = pe.PutResources(bs.ctx, resources...)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}
		err = pe.EndSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}

		// Create modified subset for upsert
		rand.New(rand.NewSource(time.Now().UnixNano()))
		upsertResources := make([]*v2.Resource, 0, upsertCount)
		for i := 0; i < upsertCount; i++ {
			// Pick random resource to update
			originalIdx := rand.Intn(setupCount)
			updatedResource := &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: fmt.Sprintf("upsert_type_%d", originalIdx%10),
					Resource:     fmt.Sprintf("upsert_resource_%d", originalIdx),
				},
				DisplayName: fmt.Sprintf("Updated Upsert Resource %d - %d", originalIdx, time.Now().UnixNano()),
			}
			upsertResources = append(upsertResources, updatedResource)
		}

		// Measure only the upsert operations
		result := bs.runBenchmark(testName, "pebble", func(eng engine.StorageEngine) error {
			_, err := eng.StartNewSync(bs.ctx)
			if err != nil {
				return err
			}

			err = eng.PutResources(bs.ctx, upsertResources...)
			if err != nil {
				return err
			}

			return eng.EndSync(bs.ctx)
		}, pe)

		result.Operations = int64(upsertCount)
		result.updateAllocsPerOp()
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}

	// Test SQLite
	se, sCleanup, err := bs.createSQLiteEngine(testName + "_sqlite")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "sqlite",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer sCleanup()

		// Setup initial data (outside measurement)
		resources := generateResources(setupCount, "upsert")
		_, err := se.StartNewSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}
		err = se.PutResources(bs.ctx, resources...)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}
		err = se.EndSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}

		// Create modified subset for upsert
		rand.New(rand.NewSource(time.Now().UnixNano()))
		upsertResources := make([]*v2.Resource, 0, upsertCount)
		for i := 0; i < upsertCount; i++ {
			// Pick random resource to update
			originalIdx := rand.Intn(setupCount)
			updatedResource := &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: fmt.Sprintf("upsert_type_%d", originalIdx%10),
					Resource:     fmt.Sprintf("upsert_resource_%d", originalIdx),
				},
				DisplayName: fmt.Sprintf("Updated Upsert Resource %d - %d", originalIdx, time.Now().UnixNano()),
			}
			upsertResources = append(upsertResources, updatedResource)
		}

		// Measure only the upsert operations
		result := bs.runBenchmark(testName, "sqlite", func(eng engine.StorageEngine) error {
			_, err := eng.StartNewSync(bs.ctx)
			if err != nil {
				return err
			}

			err = eng.PutResources(bs.ctx, upsertResources...)
			if err != nil {
				return err
			}

			return eng.EndSync(bs.ctx)
		}, se)

		result.Operations = int64(upsertCount)
		result.updateAllocsPerOp()
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}
}

// BenchmarkCleanupSync tests cleanup performance after loading data
func (bs *BenchmarkSuite) BenchmarkCleanupSync(setupCount int) {
	testName := fmt.Sprintf("CleanupSync_%d", setupCount)

	// Test Pebble
	pe, pCleanup, err := bs.createPebbleEngine(testName + "_pebble")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "pebble",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer pCleanup()

		// Setup initial data (outside measurement)
		resources := generateResources(setupCount, "cleanup")
		grants := generateGrants(setupCount/2, "cleanup") // Add some grants too

		_, err := pe.StartNewSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}
		err = pe.PutResources(bs.ctx, resources...)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}
		err = pe.PutGrants(bs.ctx, grants...)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}
		err = pe.EndSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "pebble", Error: err.Error()})
			return
		}

		// Measure only the cleanup operations
		result := bs.runBenchmark(testName, "pebble", func(eng engine.StorageEngine) error {
			return eng.Cleanup(bs.ctx)
		}, pe)

		result.Operations = int64(setupCount + setupCount/2) // Total items cleaned up
		result.updateAllocsPerOp()
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}

	// Test SQLite
	se, sCleanup, err := bs.createSQLiteEngine(testName + "_sqlite")
	if err != nil {
		bs.addResult(BenchmarkResult{
			Name:   testName,
			Engine: "sqlite",
			Error:  fmt.Sprintf("failed to create engine: %v", err),
		})
	} else {
		defer sCleanup()

		// Setup initial data (outside measurement)
		resources := generateResources(setupCount, "cleanup")
		grants := generateGrants(setupCount/2, "cleanup") // Add some grants too

		_, err := se.StartNewSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}
		err = se.PutResources(bs.ctx, resources...)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}
		err = se.PutGrants(bs.ctx, grants...)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}
		err = se.EndSync(bs.ctx)
		if err != nil {
			bs.addResult(BenchmarkResult{Name: testName, Engine: "sqlite", Error: err.Error()})
			return
		}

		// Measure only the cleanup operations
		result := bs.runBenchmark(testName, "sqlite", func(eng engine.StorageEngine) error {
			return eng.Cleanup(bs.ctx)
		}, se)

		result.Operations = int64(setupCount + setupCount/2) // Total items cleaned up
		result.updateAllocsPerOp()
		if result.Error == "" && result.Duration > 0 {
			result.OpsPerSecond = float64(result.Operations) / result.Duration.Seconds()
		}

		bs.addResult(result)
	}
}

// generateComparisons analyzes the results and creates comparisons
func (bs *BenchmarkSuite) GenerateComparisons() {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	// Create a map for easy lookup of results by test name
	pebbleResults := make(map[string]BenchmarkResult)
	sqliteResults := make(map[string]BenchmarkResult)

	for _, result := range bs.report.PebbleResults {
		if result.Error == "" {
			pebbleResults[result.Name] = result
		}
	}

	for _, result := range bs.report.SQLiteResults {
		if result.Error == "" {
			sqliteResults[result.Name] = result
		}
	}

	// Generate comparisons for tests that ran successfully on both engines
	for testName, pebbleResult := range pebbleResults {
		if sqliteResult, exists := sqliteResults[testName]; exists {
			comparison := Comparison{
				TestName:          testName,
				PebbleOpsPerSec:   pebbleResult.OpsPerSecond,
				SQLiteOpsPerSec:   sqliteResult.OpsPerSecond,
				PebbleAllocsPerOp: pebbleResult.AllocsPerOp,
				SQLiteAllocsPerOp: sqliteResult.AllocsPerOp,
			}

			// Calculate speedup ratio (positive = Pebble faster, negative = SQLite faster)
			if sqliteResult.OpsPerSecond > 0 {
				comparison.SpeedupRatio = (pebbleResult.OpsPerSecond - sqliteResult.OpsPerSecond) / sqliteResult.OpsPerSecond
			}

			// Calculate alloc ratio (positive = Pebble uses more, negative = SQLite uses more)
			if sqliteResult.AllocsPerOp > 0 {
				comparison.AllocRatio = (pebbleResult.AllocsPerOp - sqliteResult.AllocsPerOp) / sqliteResult.AllocsPerOp
				comparison.AllocPercentageDiff = comparison.AllocRatio * 100
			}

			// Determine winner based on ops/sec
			if pebbleResult.OpsPerSecond > sqliteResult.OpsPerSecond {
				comparison.Winner = "pebble"
			} else {
				comparison.Winner = "sqlite"
			}

			bs.report.Comparisons = append(bs.report.Comparisons, comparison)
		}
	}
}

// generateSummary creates a summary of the comparison results
func (bs *BenchmarkSuite) GenerateSummary() {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if len(bs.report.Comparisons) == 0 {
		return
	}

	summary := Summary{
		TotalTests: len(bs.report.Comparisons),
	}

	var totalSpeedupRatio, totalAllocRatio float64
	var totalPebbleOps, totalSQLiteOps float64

	for _, comp := range bs.report.Comparisons {
		totalSpeedupRatio += comp.SpeedupRatio
		totalAllocRatio += comp.AllocRatio
		totalPebbleOps += comp.PebbleOpsPerSec
		totalSQLiteOps += comp.SQLiteOpsPerSec

		if comp.Winner == "pebble" {
			summary.PebbleWins++
		} else {
			summary.SQLiteWins++
		}
	}

	summary.AverageSpeedupRatio = totalSpeedupRatio / float64(summary.TotalTests)
	summary.AverageAllocRatio = totalAllocRatio / float64(summary.TotalTests)
	summary.PebbleAvgOpsPerSec = totalPebbleOps / float64(summary.TotalTests)
	summary.SQLiteAvgOpsPerSec = totalSQLiteOps / float64(summary.TotalTests)

	// Make recommendation
	if summary.PebbleWins > summary.SQLiteWins {
		summary.RecommendedEngine = "pebble"
		summary.RecommendationReason = fmt.Sprintf("Pebble won %d/%d tests with %.2f%% better average performance",
			summary.PebbleWins, summary.TotalTests, summary.AverageSpeedupRatio*100)
	} else {
		summary.RecommendedEngine = "sqlite"
		summary.RecommendationReason = fmt.Sprintf("SQLite won %d/%d tests with %.2f%% better average performance",
			summary.SQLiteWins, summary.TotalTests, -summary.AverageSpeedupRatio*100)
	}

	bs.report.Summary = summary
}

// SaveReport saves the comparison report to a JSON file
func (bs *BenchmarkSuite) SaveReport(filename string) error {
	data, err := json.MarshalIndent(bs.report, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}

// PrintSummary prints a human-readable summary to stdout
func (bs *BenchmarkSuite) PrintSummary() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("🏁 ENGINE COMPARISON SUMMARY")
	fmt.Println(strings.Repeat("=", 80))

	summary := bs.report.Summary

	fmt.Printf("📊 Total Tests: %d\n", summary.TotalTests)
	fmt.Printf("🥇 Pebble Wins: %d\n", summary.PebbleWins)
	fmt.Printf("🥈 SQLite Wins: %d\n", summary.SQLiteWins)
	fmt.Printf("⚡ Average Speedup Ratio: %.2f%% (positive = Pebble faster)\n", summary.AverageSpeedupRatio*100)
	fmt.Printf("🔢 Average Alloc Ratio: %.2f%% (positive = Pebble uses more per op)\n", summary.AverageAllocRatio*100)
	fmt.Printf("🚀 Pebble Avg Ops/Sec: %.2f\n", summary.PebbleAvgOpsPerSec)
	fmt.Printf("🗃️  SQLite Avg Ops/Sec: %.2f\n", summary.SQLiteAvgOpsPerSec)
	fmt.Printf("🏆 Recommended Engine: %s\n", summary.RecommendedEngine)
	fmt.Printf("💡 Reason: %s\n", summary.RecommendationReason)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📈 DETAILED RESULTS")
	fmt.Println(strings.Repeat("=", 80))

	for _, comp := range bs.report.Comparisons {
		fmt.Printf("🧪 %s:\n", comp.TestName)
		fmt.Printf("   Pebble: %.2f ops/sec, %.2f allocs/op\n", 
			comp.PebbleOpsPerSec, comp.PebbleAllocsPerOp)
		fmt.Printf("   SQLite: %.2f ops/sec, %.2f allocs/op\n", 
			comp.SQLiteOpsPerSec, comp.SQLiteAllocsPerOp)
		fmt.Printf("   Winner: %s (%.2f%% faster)\n", comp.Winner, abs(comp.SpeedupRatio)*100)
		fmt.Printf("   Allocs/Op Diff: %.2f%% (positive = Pebble uses more)\n", comp.AllocPercentageDiff)
		fmt.Println()
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// formatBytes converts bytes to human readable format
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// Test function that can be run with `go test`
func TestEngineComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping engine comparison in short mode")
	}

	ctx := context.Background()
	suite := NewBenchmarkSuite(ctx)

	fmt.Printf("🚀 Engine Comparison Benchmark Suite\n")
	fmt.Printf("📁 Results will be saved to: %s\n", *benchOutput)
	fmt.Printf("📏 Scale: %s\n", *benchScale)
	fmt.Println()

	start := time.Now()

	// Run benchmarks based on scale
	switch *benchScale {
	case "small":
		fmt.Println("🔬 Running small scale benchmarks...")
		suite.BenchmarkRawInserts(1000)
		suite.BenchmarkSequentialReads(1000, 100)
		suite.BenchmarkRandomReads(1000, 100)
		suite.BenchmarkUpsertIfNewer(1000, 100)
		suite.BenchmarkCleanupSync(1000)
	case "medium":
		fmt.Println("⚖️ Running medium scale benchmarks...")
		suite.BenchmarkRawInserts(10000)
		suite.BenchmarkRawInserts(50000)
		suite.BenchmarkSequentialReads(10000, 1000)
		suite.BenchmarkSequentialReads(10000, 5000)
		suite.BenchmarkRandomReads(10000, 1000)
		suite.BenchmarkRandomReads(10000, 5000)
		suite.BenchmarkUpsertIfNewer(10000, 1000)
		suite.BenchmarkUpsertIfNewer(50000, 5000)
		suite.BenchmarkCleanupSync(10000)
		suite.BenchmarkCleanupSync(50000)
	case "large":
		fmt.Println("🏗️ Running large scale benchmarks...")
		suite.BenchmarkRawInserts(100000)
		suite.BenchmarkRawInserts(500000)
		suite.BenchmarkSequentialReads(100000, 10000)
		suite.BenchmarkSequentialReads(100000, 50000)
		suite.BenchmarkRandomReads(100000, 10000)
		suite.BenchmarkRandomReads(100000, 25000)
		suite.BenchmarkUpsertIfNewer(100000, 10000)
		suite.BenchmarkUpsertIfNewer(500000, 50000)
		suite.BenchmarkCleanupSync(100000)
		suite.BenchmarkCleanupSync(500000)
	case "extralarge":
		fmt.Println("🚀 Running extra large scale benchmarks...")
		suite.BenchmarkRawInserts(1000000)
		suite.BenchmarkRawInserts(5000000)
		suite.BenchmarkSequentialReads(1000000, 100000)
		suite.BenchmarkSequentialReads(1000000, 500000)
		suite.BenchmarkRandomReads(1000000, 100000)
		suite.BenchmarkRandomReads(1000000, 250000)
		suite.BenchmarkUpsertIfNewer(1000000, 100000)
		suite.BenchmarkUpsertIfNewer(5000000, 500000)
		suite.BenchmarkCleanupSync(1000000)
		suite.BenchmarkCleanupSync(5000000)
	default:
		t.Fatalf("Unknown scale: %s (options: small, medium, large, extralarge)", *benchScale)
	}

	duration := time.Since(start)

	// Generate analysis
	suite.GenerateComparisons()
	suite.GenerateSummary()

	// Save detailed results
	err := suite.SaveReport(*benchOutput)
	require.NoError(t, err)

	fmt.Printf("\n⏱️  Total runtime: %v\n", duration)
	fmt.Printf("💾 Detailed results saved to: %s\n", *benchOutput)

	// Print summary
	suite.PrintSummary()

	fmt.Println("✅ Benchmark comparison completed successfully!")

	// Basic assertions to ensure tests ran
	require.NotEmpty(t, suite.report.PebbleResults, "Should have Pebble results")
	require.NotEmpty(t, suite.report.SQLiteResults, "Should have SQLite results")
	require.NotEmpty(t, suite.report.Comparisons, "Should have comparisons")
}
