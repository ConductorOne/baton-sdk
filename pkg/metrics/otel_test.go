package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type OtelHandlerTestSuite struct {
	suite.Suite
	reader   sdkmetric.Reader
	exporter sdkmetric.Exporter
	handler  Handler
	out      *bytes.Buffer
}

type metricsData struct {
	ScopeMetrics []struct {
		Metrics []struct {
			Name        string `json:"Name"`
			Description string `json:"Description"`
			Unit        string `json:"Unit"`
			Data        struct {
				DataPoints []struct {
					Attributes []struct {
						Key   string `json:"Key"`
						Value any    `json:"Value"`
					} `json:"Attributes"`
					Value        float64  `json:"Value,omitempty"`
					BucketCounts []uint64 `json:"BucketCounts,omitempty"`
				} `json:"DataPoints"`
			} `json:"Data"`
		} `json:"Metrics"`
	} `json:"ScopeMetrics"`
}

func (suite *OtelHandlerTestSuite) SetupTest() {
	suite.out = new(bytes.Buffer)
	exp, err := stdoutmetric.New(stdoutmetric.WithEncoder(json.NewEncoder(suite.out)), stdoutmetric.WithoutTimestamps())
	assert.NoError(suite.T(), err)
	suite.exporter = exp
	suite.reader = sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(suite.reader))
	suite.handler = NewOtelHandler(context.TODO(), provider, "test")
}

func (suite *OtelHandlerTestSuite) TestInt64Counter_noattrs() {
	ctx := context.TODO()
	var counter Int64Counter
	assert.NotPanics(suite.T(), func() {
		counter = suite.handler.Int64Counter("test_counter", "A counter for tests", Dimensionless)
	})
	assert.NotPanics(suite.T(), func() {
		counter.Add(ctx, 1, nil)
	})
	var rm metricdata.ResourceMetrics
	err := suite.reader.Collect(ctx, &rm)
	assert.NoError(suite.T(), err)
	err = suite.exporter.Export(ctx, &rm)
	assert.NoError(suite.T(), err)
	var data metricsData
	err = json.Unmarshal(suite.out.Bytes(), &data)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "test_counter", data.ScopeMetrics[0].Metrics[0].Name)
	assert.Equal(suite.T(), 0, len(data.ScopeMetrics[0].Metrics[0].Data.DataPoints[0].Attributes))
}

func (suite *OtelHandlerTestSuite) TestInt64Counter_withattrs() {
	ctx := context.TODO()
	var counter Int64Counter
	assert.NotPanics(suite.T(), func() {
		counter = suite.handler.Int64Counter("test_counter", "A counter for tests", Dimensionless)
	})
	assert.NotPanics(suite.T(), func() {
		counter.Add(ctx, 1, map[string]string{"key": "value"})
	})
	var rm metricdata.ResourceMetrics
	err := suite.reader.Collect(ctx, &rm)
	assert.NoError(suite.T(), err)
	err = suite.exporter.Export(ctx, &rm)
	assert.NoError(suite.T(), err)
	var data metricsData
	err = json.Unmarshal(suite.out.Bytes(), &data)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "test_counter", data.ScopeMetrics[0].Metrics[0].Name)
	assert.Equal(suite.T(), 1, len(data.ScopeMetrics[0].Metrics[0].Data.DataPoints[0].Attributes))
}

func (suite *OtelHandlerTestSuite) TestInt64Gauge_noattrs() {
	ctx := context.TODO()
	var gauge Int64Gauge
	assert.NotPanics(suite.T(), func() {
		gauge = suite.handler.Int64Gauge("test_gauge", "A gauge for tests", Dimensionless)
	})
	assert.NotPanics(suite.T(), func() {
		gauge.Observe(ctx, 1, nil)
	})
	var rm metricdata.ResourceMetrics
	err := suite.reader.Collect(ctx, &rm)
	assert.NoError(suite.T(), err)
	err = suite.exporter.Export(ctx, &rm)
	assert.NoError(suite.T(), err)
	var data metricsData
	err = json.Unmarshal(suite.out.Bytes(), &data)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "test_gauge", data.ScopeMetrics[0].Metrics[0].Name)
	assert.Equal(suite.T(), 0, len(data.ScopeMetrics[0].Metrics[0].Data.DataPoints[0].Attributes))
}

func (suite *OtelHandlerTestSuite) TestInt64Gauge_withattrs() {
	ctx := context.TODO()
	var gauge Int64Gauge
	assert.NotPanics(suite.T(), func() {
		gauge = suite.handler.Int64Gauge("test_gauge", "A gauge for tests", Dimensionless)
	})
	assert.NotPanics(suite.T(), func() {
		gauge.Observe(ctx, 1, map[string]string{
			"key": "value",
		})
	})
	var rm metricdata.ResourceMetrics
	err := suite.reader.Collect(ctx, &rm)
	assert.NoError(suite.T(), err)
	err = suite.exporter.Export(ctx, &rm)
	assert.NoError(suite.T(), err)
	var data metricsData
	err = json.Unmarshal(suite.out.Bytes(), &data)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "test_gauge", data.ScopeMetrics[0].Metrics[0].Name)
	assert.Equal(suite.T(), 1, len(data.ScopeMetrics[0].Metrics[0].Data.DataPoints[0].Attributes))
}

func (suite *OtelHandlerTestSuite) TestWithTags() {
	ctx := context.TODO()
	var newHandler Handler
	assert.NotPanics(suite.T(), func() {
		newHandler = suite.handler.WithTags(map[string]string{"default_key": "default_value"})
	})
	assert.NotPanics(suite.T(), func() {
		counter := newHandler.Int64Counter("test_counter_with_defaults", "A counter for tests", Dimensionless)
		counter.Add(ctx, 1, map[string]string{"key": "value"})
	})
	var rm metricdata.ResourceMetrics
	err := suite.reader.Collect(ctx, &rm)
	assert.NoError(suite.T(), err)
	err = suite.exporter.Export(ctx, &rm)
	assert.NoError(suite.T(), err)
	var data metricsData
	err = json.Unmarshal(suite.out.Bytes(), &data)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "test_counter_with_defaults", data.ScopeMetrics[0].Metrics[0].Name)
	assert.Equal(suite.T(), 2, len(data.ScopeMetrics[0].Metrics[0].Data.DataPoints[0].Attributes))
}

func TestOtelHandler(t *testing.T) {
	suite.Run(t, new(OtelHandlerTestSuite))
}
