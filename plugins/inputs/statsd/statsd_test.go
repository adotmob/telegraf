package statsd

import (
	"errors"
	"fmt"
	"testing"

	"github.com/influxdata/telegraf/testutil"
)

func NewTestStatsd() *Statsd {
	s := Statsd{}

	// Make data structures
	s.done = make(chan struct{})
	s.in = make(chan []byte, s.AllowedPendingMessages)
	s.gauges = make(map[string]cachedgauge)
	s.counters = make(map[string]cachedcounter)
	s.sets = make(map[string]cachedset)
	s.timings = make(map[string]cachedtimings)

	s.MetricSeparator = "_"

	return &s
}

// Valid lines should be parsed and their values should be cached
func TestParse_ValidLines(t *testing.T) {
	s := NewTestStatsd()
	valid_lines := []string{
		"valid:45|c",
		"valid:45|s",
		"valid:45|g",
		"valid.timer:45|ms",
		"valid.timer:45|h",
	}

	for _, line := range valid_lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}
}

// Tests low-level functionality of gauges
func TestParse_Gauges(t *testing.T) {
	s := NewTestStatsd()

	// Test that gauge +- values work
	valid_lines := []string{
		"plus.minus:100|g",
		"plus.minus:-10|g",
		"plus.minus:+30|g",
		"plus.plus:100|g",
		"plus.plus:+100|g",
		"plus.plus:+100|g",
		"minus.minus:100|g",
		"minus.minus:-100|g",
		"minus.minus:-100|g",
		"lone.plus:+100|g",
		"lone.minus:-100|g",
		"overwrite:100|g",
		"overwrite:300|g",
		"scientific.notation:4.696E+5|g",
		"scientific.notation.minus:4.7E-5|g",
	}

	for _, line := range valid_lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	validations := []struct {
		name  string
		value float64
	}{
		{
			"scientific_notation",
			469600,
		},
		{
			"scientific_notation_minus",
			0.000047,
		},
		{
			"plus_minus",
			120,
		},
		{
			"plus_plus",
			300,
		},
		{
			"minus_minus",
			-100,
		},
		{
			"lone_plus",
			100,
		},
		{
			"lone_minus",
			-100,
		},
		{
			"overwrite",
			300,
		},
	}

	for _, test := range validations {
		err := test_validate_gauge(test.name, test.value, s.gauges)
		if err != nil {
			t.Error(err.Error())
		}
	}
}

// Tests low-level functionality of sets
func TestParse_Sets(t *testing.T) {
	s := NewTestStatsd()

	// Test that sets work
	valid_lines := []string{
		"unique.user.ids:100|s",
		"unique.user.ids:100|s",
		"unique.user.ids:100|s",
		"unique.user.ids:100|s",
		"unique.user.ids:100|s",
		"unique.user.ids:101|s",
		"unique.user.ids:102|s",
		"unique.user.ids:102|s",
		"unique.user.ids:123456789|s",
		"oneuser.id:100|s",
		"oneuser.id:100|s",
		"scientific.notation.sets:4.696E+5|s",
		"scientific.notation.sets:4.696E+5|s",
		"scientific.notation.sets:4.697E+5|s",
		"string.sets:foobar|s",
		"string.sets:foobar|s",
		"string.sets:bar|s",
	}

	for _, line := range valid_lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"scientific_notation_sets",
			2,
		},
		{
			"unique_user_ids",
			4,
		},
		{
			"oneuser_id",
			1,
		},
		{
			"string_sets",
			2,
		},
	}

	for _, test := range validations {
		err := test_validate_set(test.name, test.value, s.sets)
		if err != nil {
			t.Error(err.Error())
		}
	}
}

// Tests low-level functionality of counters
func TestParse_Counters(t *testing.T) {
	s := NewTestStatsd()

	// Test that counters work
	valid_lines := []string{
		"small.inc:1|c",
		"big.inc:100|c",
		"big.inc:1|c",
		"big.inc:100000|c",
		"big.inc:1000000|c",
		"small.inc:1|c",
		"zero.init:0|c",
		"sample.rate:1|c|@0.1",
		"sample.rate:1|c",
		"scientific.notation:4.696E+5|c",
		"negative.test:100|c",
		"negative.test:-5|c",
	}

	for _, line := range valid_lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"scientific_notation",
			469600,
		},
		{
			"small_inc",
			2,
		},
		{
			"big_inc",
			1100101,
		},
		{
			"zero_init",
			0,
		},
		{
			"sample_rate",
			11,
		},
		{
			"negative_test",
			95,
		},
	}

	for _, test := range validations {
		err := test_validate_counter(test.name, test.value, s.counters)
		if err != nil {
			t.Error(err.Error())
		}
	}
}

// Tests low-level functionality of timings
func TestParse_Timings(t *testing.T) {
	s := NewTestStatsd()
	s.Percentiles = []int{90}
	acc := &testutil.Accumulator{}

	// Test that counters work
	valid_lines := []string{
		"test.timing:1|ms",
		"test.timing:11|ms",
		"test.timing:1|ms",
		"test.timing:1|ms",
		"test.timing:1|ms",
	}

	for _, line := range valid_lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	s.Gather(acc)

	valid := map[string]interface{}{
		"90_percentile": float64(11),
		"count":         int64(5),
		"lower":         float64(1),
		"mean":          float64(3),
		"stddev":        float64(4),
		"upper":         float64(11),
	}

	acc.AssertContainsFields(t, "test_timing", valid)
}

func TestParseScientificNotation(t *testing.T) {
	s := NewTestStatsd()
	sciNotationLines := []string{
		"scientific.notation:4.6968460083008E-5|ms",
		"scientific.notation:4.6968460083008E-5|g",
		"scientific.notation:4.6968460083008E-5|c",
		"scientific.notation:4.6968460083008E-5|h",
	}
	for _, line := range sciNotationLines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line [%s] should not have resulted in error: %s\n", line, err)
		}
	}
}

// Invalid lines should return an error
func TestParse_InvalidLines(t *testing.T) {
	s := NewTestStatsd()
	invalid_lines := []string{
		"i.dont.have.a.pipe:45g",
		"i.dont.have.a.colon45|c",
		"invalid.metric.type:45|e",
		"invalid.plus.minus.non.gauge:+10|s",
		"invalid.plus.minus.non.gauge:+10|ms",
		"invalid.plus.minus.non.gauge:+10|h",
		"invalid.value:foobar|c",
		"invalid.value:d11|c",
		"invalid.value:1d1|c",
	}
	for _, line := range invalid_lines {
		err := s.parseStatsdLine(line)
		if err == nil {
			t.Errorf("Parsing line %s should have resulted in an error\n", line)
		}
	}
}

// Invalid sample rates should be ignored and not applied
func TestParse_InvalidSampleRate(t *testing.T) {
	s := NewTestStatsd()
	invalid_lines := []string{
		"invalid.sample.rate:45|c|0.1",
		"invalid.sample.rate.2:45|c|@foo",
		"invalid.sample.rate:45|g|@0.1",
		"invalid.sample.rate:45|s|@0.1",
	}

	for _, line := range invalid_lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	counter_validations := []struct {
		name  string
		value int64
		cache map[string]cachedcounter
	}{
		{
			"invalid_sample_rate",
			45,
			s.counters,
		},
		{
			"invalid_sample_rate_2",
			45,
			s.counters,
		},
	}

	for _, test := range counter_validations {
		err := test_validate_counter(test.name, test.value, test.cache)
		if err != nil {
			t.Error(err.Error())
		}
	}

	err := test_validate_gauge("invalid_sample_rate", 45, s.gauges)
	if err != nil {
		t.Error(err.Error())
	}

	err = test_validate_set("invalid_sample_rate", 1, s.sets)
	if err != nil {
		t.Error(err.Error())
	}
}

// Names should be parsed like . -> _
func TestParse_DefaultNameParsing(t *testing.T) {
	s := NewTestStatsd()
	valid_lines := []string{
		"valid:1|c",
		"valid.foo-bar:11|c",
	}

	for _, line := range valid_lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"valid",
			1,
		},
		{
			"valid_foo-bar",
			11,
		},
	}

	for _, test := range validations {
		err := test_validate_counter(test.name, test.value, s.counters)
		if err != nil {
			t.Error(err.Error())
		}
	}
}

// Test that template name transformation works
func TestParse_Template(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{
		"measurement.measurement.host.service",
	}

	lines := []string{
		"cpu.idle.localhost:1|c",
		"cpu.busy.host01.myservice:11|c",
	}

	for _, line := range lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"cpu_idle",
			1,
		},
		{
			"cpu_busy",
			11,
		},
	}

	// Validate counters
	for _, test := range validations {
		err := test_validate_counter(test.name, test.value, s.counters)
		if err != nil {
			t.Error(err.Error())
		}
	}
}

// Test that template filters properly
func TestParse_TemplateFilter(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{
		"cpu.idle.* measurement.measurement.host",
		"druid.*.segment.loadQueue.*.* measurement.druid_role.field.field.field.server",
		"druid.*.segment.dropQueue.*.* measurement.druid_role.field.field.field.server",
		"druid.*.segment.*.*.* measurement.druid_role.field.field.field.tier",
		"druid.*.segment.*.* measurement.druid_role.field.field.field",
		"druid.*.segment.size.* measurement.druid_role.field.field.datasource",
		"druid.*.query.cache.*.* measurement.druid_role.field.field.field.field",
		"druid.*.query.*.*.* measurement.druid_role.field.field.datasource.query_type",
		"druid.*.jvm.*.* measurement.druid_role.field.field.field",
	}

	lines := []string{
		"cpu.busy.host01.myservice:11|c",
		"druid.broker.jvm.bufferpool.capacity:0|g",
		"druid.broker.jvm.bufferpool.capacity:1028325437|g",
		"druid.broker.jvm.bufferpool.count:0|g",
		"druid.broker.jvm.bufferpool.count:106|g",
		"druid.broker.jvm.bufferpool.used:0|g",
		"druid.broker.jvm.bufferpool.used:1028325437|g",
		"druid.broker.jvm.gc.count.PS MarkSweep:0|c",
		"druid.broker.jvm.gc.count.PS Scavenge:0|c",
		"druid.broker.jvm.gc.time.PS MarkSweep:0|ms",
		"druid.broker.jvm.gc.time.PS Scavenge:0|ms",
		"druid.broker.jvm.mem.committed.heap:1051197440|g",
		"druid.broker.jvm.mem.committed.nonheap:79626240|g",
		"druid.broker.jvm.mem.init.heap:1073741824|g",
		"druid.broker.jvm.mem.init.nonheap:2555904|g",
		"druid.broker.jvm.mem.max.heap:1051197440|g",
		"druid.broker.jvm.mem.max.nonheap:-1|g",
		"druid.broker.jvm.mem.max.nonheap:0|g",
		"druid.broker.jvm.mem.used.heap:548661880|g",
		"druid.broker.jvm.mem.used.nonheap:78027720|g",
		"druid.broker.jvm.pool.committed.heap.PS Eden Space:311427072|g",
		"druid.broker.jvm.pool.committed.heap.PS Old Gen:716177408|g",
		"druid.broker.jvm.pool.committed.heap.PS Survivor Space:23592960|g",
		"druid.broker.jvm.pool.committed.nonheap.Code Cache:23527424|g",
		"druid.broker.jvm.pool.committed.nonheap.Compressed Class Space:6160384|g",
		"druid.broker.jvm.pool.committed.nonheap.Metaspace:49938432|g",
		"druid.broker.jvm.pool.init.heap.PS Eden Space:268435456|g",
		"druid.broker.jvm.pool.init.heap.PS Old Gen:716177408|g",
		"druid.broker.jvm.pool.init.heap.PS Survivor Space:44564480|g",
		"druid.broker.jvm.pool.init.nonheap.Code Cache:2555904|g",
		"druid.broker.jvm.pool.init.nonheap.Compressed Class Space:0|g",
		"druid.broker.jvm.pool.init.nonheap.Metaspace:0|g",
		"druid.broker.jvm.pool.max.heap.PS Eden Space:311427072|g",
		"druid.broker.jvm.pool.max.heap.PS Old Gen:716177408|g",
		"druid.broker.jvm.pool.max.heap.PS Survivor Space:23592960|g",
		"druid.broker.jvm.pool.max.nonheap.Code Cache:251658240|g",
		"druid.broker.jvm.pool.max.nonheap.Compressed Class Space:1073741824|g",
		"druid.broker.jvm.pool.max.nonheap.Metaspace:-1|g",
		"druid.broker.jvm.pool.max.nonheap.Metaspace:0|g",
		"druid.broker.jvm.pool.used.heap.PS Eden Space:48250288|g",
		"druid.broker.jvm.pool.used.heap.PS Old Gen:497443704|g",
		"druid.broker.jvm.pool.used.heap.PS Survivor Space:2967888|g",
		"druid.broker.jvm.pool.used.nonheap.Code Cache:23157760|g",
		"druid.broker.jvm.pool.used.nonheap.Compressed Class Space:5888736|g",
		"druid.broker.jvm.pool.used.nonheap.Metaspace:48981224|g",
		"druid.broker.query.cache.delta.averageBytes:0|c",
		"druid.broker.query.cache.delta.errors:0|c",
		"druid.broker.query.cache.delta.evictions:0|c",
		"druid.broker.query.cache.delta.hitRate:0|c",
		"druid.broker.query.cache.delta.hits:0|c",
		"druid.broker.query.cache.delta.misses:0|c",
		"druid.broker.query.cache.delta.numEntries:0|c",
		"druid.broker.query.cache.delta.sizeBytes:0|c",
		"druid.broker.query.cache.delta.timeouts:0|c",
		"druid.broker.query.cache.total.averageBytes:2489|g",
		"druid.broker.query.cache.total.errors:0|g",
		"druid.broker.query.cache.total.evictions:12610|g",
		"druid.broker.query.cache.total.hitRate:0|g",
		"druid.broker.query.cache.total.hits:1037|g",
		"druid.broker.query.cache.total.misses:16651|g",
		"druid.broker.query.cache.total.numEntries:4014|g",
		"druid.broker.query.cache.total.sizeBytes:9991652|g",
		"druid.broker.query.cache.total.timeouts:0|g",
		"druid.broker.query.cpu.time.metrics-realtime.segmentMetadata:19780|ms",
		"druid.broker.query.node.time.ip-172-16-31-109.eu-west-1.compute.internal.8083:138|ms",
		"druid.broker.query.node.time.ip-172-16-31-109.eu-west-1.compute.internal.8084:4|ms",
		"druid.broker.query.node.ttfb.ip-172-16-31-109.eu-west-1.compute.internal.8083:25|ms",
		"druid.broker.query.node.ttfb.ip-172-16-31-109.eu-west-1.compute.internal.8084:4|ms",
		"druid.broker.segment.scan.pending:0|g",
		"druid.coordinator.segment.assigned.count._default_tier:0|c",
		"druid.coordinator.segment.dropQueue.count.ip-172-16-31-109.eu-west-1.compute.internal.8083:0|g",
		"druid.coordinator.segment.dropped.count._default_tier:0|c",
		"druid.coordinator.segment.loadQueue.count.ip-172-16-31-109.eu-west-1.compute.internal.8083:0|g",
		"druid.coordinator.segment.loadQueue.failed.ip-172-16-31-109.eu-west-1.compute.internal.8083:0|g",
		"druid.coordinator.segment.loadQueue.size.ip-172-16-31-109.eu-west-1.compute.internal.8083:0|g",
		"druid.coordinator.segment.overShadowed.count:0|g",
		"druid.coordinator.segment.size.metrics-realtime:244135778|g",
		"druid.coordinator.task.run.time.metrics-supervisor.index_kafka:-1|ms",
		"druid.historical.jvm.bufferpool.capacity:1027125354|g",
		"druid.historical.jvm.bufferpool.capacity:242629144|g",
		"druid.historical.jvm.bufferpool.count:64|g",
		"druid.historical.jvm.bufferpool.used:1027125354|g",
		"druid.historical.jvm.bufferpool.used:242629144|g",
		"druid.historical.jvm.gc.count.PS Scavenge:0|c",
		"druid.historical.jvm.gc.time.PS MarkSweep:0|ms",
		"druid.historical.jvm.mem.committed.heap:1069023232|g",
		"druid.historical.jvm.mem.committed.nonheap:78970880|g",
		"druid.historical.jvm.mem.init.heap:1073741824|g",
		"druid.historical.jvm.mem.init.nonheap:2555904|g",
		"druid.historical.jvm.mem.max.heap:1069023232|g",
		"druid.historical.jvm.mem.max.nonheap:-1|g",
		"druid.historical.jvm.mem.max.nonheap:0|g",
		"druid.historical.jvm.mem.used.heap:689411264|g",
		"druid.historical.jvm.mem.used.nonheap:77251232|g",
		"druid.historical.jvm.pool.committed.heap.PS Eden Space:348127232|g",
		"druid.historical.jvm.pool.committed.heap.PS Old Gen:716177408|g",
		"druid.historical.jvm.pool.committed.heap.PS Survivor Space:4718592|g",
		"druid.historical.jvm.pool.committed.nonheap.Code Cache:27066368|g",
		"druid.historical.jvm.pool.committed.nonheap.Compressed Class Space:5505024|g",
		"druid.historical.jvm.pool.committed.nonheap.Metaspace:46399488|g",
		"druid.historical.jvm.pool.init.heap.PS Eden Space:268435456|g",
		"druid.historical.jvm.pool.init.heap.PS Old Gen:716177408|g",
		"druid.historical.jvm.pool.init.heap.PS Survivor Space:44564480|g",
		"druid.historical.jvm.pool.init.nonheap.Code Cache:2555904|g",
		"druid.historical.jvm.pool.init.nonheap.Metaspace:0|g",
		"druid.historical.jvm.pool.max.heap.PS Old Gen:716177408|g",
		"druid.historical.jvm.pool.max.heap.PS Survivor Space:4718592|g",
		"druid.historical.jvm.pool.max.nonheap.Code Cache:251658240|g",
		"druid.historical.jvm.pool.max.nonheap.Compressed Class Space:1073741824|g",
		"druid.historical.jvm.pool.max.nonheap.Metaspace:-1|g",
		"druid.historical.jvm.pool.max.nonheap.Metaspace:0|g",
		"druid.historical.jvm.pool.used.heap.PS Eden Space:42819560|g",
		"druid.historical.jvm.pool.used.heap.PS Old Gen:643066064|g",
		"druid.historical.jvm.pool.used.heap.PS Survivor Space:3525640|g",
		"druid.historical.jvm.pool.used.nonheap.Code Cache:26599936|g",
		"druid.historical.jvm.pool.used.nonheap.Metaspace:45374712|g",
		"druid.historical.query.cache.delta.averageBytes:0|c",
		"druid.historical.query.cache.delta.errors:0|c",
		"druid.historical.query.cache.delta.evictions:0|c",
		"druid.historical.query.cache.delta.hitRate:0|c",
		"druid.historical.query.cache.delta.hits:0|c",
		"druid.historical.query.cache.delta.misses:0|c",
		"druid.historical.query.cache.delta.numEntries:0|c",
		"druid.historical.query.cache.delta.sizeBytes:0|c",
		"druid.historical.query.cache.delta.timeouts:0|c",
		"druid.historical.query.cache.total.errors:0|g",
		"druid.historical.query.cache.total.evictions:0|g",
		"druid.historical.query.cache.total.hits:0|g",
		"druid.historical.query.cache.total.misses:0|g",
		"druid.historical.query.cache.total.numEntries:0|g",
		"druid.historical.query.cache.total.sizeBytes:0|g",
		"druid.historical.query.cpu.time.metrics-realtime.segmentMetadata:75376|ms",
		"druid.historical.query.segment.time:0|ms",
		"druid.historical.query.segment.time:1|ms",
		"druid.historical.query.segmentAndCache.time:0|ms",
		"druid.historical.query.segmentAndCache.time:1|ms",
		"druid.historical.query.time.metrics-realtime.segmentMetadata:126|ms",
		"druid.historical.query.wait.time:0|ms",
		"druid.historical.query.wait.time:1|ms",
		"druid.historical.query.wait.time:2|ms",
		"druid.historical.query.wait.time:3|ms",
		"druid.historical.segment.scan.pending:0|g",
		"druid.realtime.query.cpu.time.metrics-realtime.segmentMetadata:175|ms",
		"druid.realtime.query.cpu.time.metrics-realtime.segmentMetadata:379|ms",
		"druid.realtime.query.segment.time:0|ms",
		"druid.realtime.query.segmentAndCache.time:0|ms",
		"druid.realtime.query.time.metrics-realtime.segmentMetadata:2|ms",
		"druid.realtime.query.wait.time:0|ms",
		"druid.realtime.query.wait.time:1|ms",
	}

	for _, line := range lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	for _, item := range s.counters {
		t.Error(item)
	}

	for _, item := range s.gauges {
		t.Error(item)
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"cpu_idle",
			1,
		},
		{
			"cpu_busy_host01_myservice",
			11,
		},
		{
			"segment_assigned_count",
			0,
		},
	}

	// Validate counters
	for _, test := range validations {
		err := test_validate_counter(test.name, test.value, s.counters)
		if err != nil {
			t.Error(err.Error())
		}
	}
}

// Test that most specific template is chosen
func TestParse_TemplateSpecificity(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{
		"cpu.* measurement.foo.host",
		"cpu.idle.* measurement.measurement.host",
	}

	lines := []string{
		"cpu.idle.localhost:1|c",
	}

	for _, line := range lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	validations := []struct {
		name  string
		value int64
	}{
		{
			"cpu_idle",
			1,
		},
	}

	// Validate counters
	for _, test := range validations {
		err := test_validate_counter(test.name, test.value, s.counters)
		if err != nil {
			t.Error(err.Error())
		}
	}
}

// Test that most specific template is chosen
func TestParse_TemplateFields(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{
		"* measurement.measurement.field",
	}

	lines := []string{
		"my.counter.f1:1|c",
		"my.counter.f1:1|c",
		"my.counter.f2:1|c",
		"my.counter.f3:10|c",
		"my.counter.f3:100|c",
		"my.gauge.f1:10.1|g",
		"my.gauge.f2:10.1|g",
		"my.gauge.f1:0.9|g",
		"my.set.f1:1|s",
		"my.set.f1:2|s",
		"my.set.f1:1|s",
		"my.set.f2:100|s",
	}

	for _, line := range lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	counter_tests := []struct {
		name  string
		value int64
		field string
	}{
		{
			"my_counter",
			2,
			"f1",
		},
		{
			"my_counter",
			1,
			"f2",
		},
		{
			"my_counter",
			110,
			"f3",
		},
	}
	// Validate counters
	for _, test := range counter_tests {
		err := test_validate_counter(test.name, test.value, s.counters, test.field)
		if err != nil {
			t.Error(err.Error())
		}
	}

	gauge_tests := []struct {
		name  string
		value float64
		field string
	}{
		{
			"my_gauge",
			0.9,
			"f1",
		},
		{
			"my_gauge",
			10.1,
			"f2",
		},
	}
	// Validate gauges
	for _, test := range gauge_tests {
		err := test_validate_gauge(test.name, test.value, s.gauges, test.field)
		if err != nil {
			t.Error(err.Error())
		}
	}

	set_tests := []struct {
		name  string
		value int64
		field string
	}{
		{
			"my_set",
			2,
			"f1",
		},
		{
			"my_set",
			1,
			"f2",
		},
	}
	// Validate sets
	for _, test := range set_tests {
		err := test_validate_set(test.name, test.value, s.sets, test.field)
		if err != nil {
			t.Error(err.Error())
		}
	}
}

// Test that fields are parsed correctly
func TestParse_Fields(t *testing.T) {
	if false {
		t.Errorf("TODO")
	}
}

// Test that tags within the bucket are parsed correctly
func TestParse_Tags(t *testing.T) {
	s := NewTestStatsd()

	tests := []struct {
		bucket string
		name   string
		tags   map[string]string
	}{
		{
			"cpu.idle,host=localhost",
			"cpu_idle",
			map[string]string{
				"host": "localhost",
			},
		},
		{
			"cpu.idle,host=localhost,region=west",
			"cpu_idle",
			map[string]string{
				"host":   "localhost",
				"region": "west",
			},
		},
		{
			"cpu.idle,host=localhost,color=red,region=west",
			"cpu_idle",
			map[string]string{
				"host":   "localhost",
				"region": "west",
				"color":  "red",
			},
		},
	}

	for _, test := range tests {
		name, _, tags := s.parseName(test.bucket)
		if name != test.name {
			t.Errorf("Expected: %s, got %s", test.name, name)
		}

		for k, v := range test.tags {
			actual, ok := tags[k]
			if !ok {
				t.Errorf("Expected key: %s not found", k)
			}
			if actual != v {
				t.Errorf("Expected %s, got %s", v, actual)
			}
		}
	}
}

// Test that DataDog tags are parsed
func TestParse_DataDogTags(t *testing.T) {
	s := NewTestStatsd()
	s.ParseDataDogTags = true

	lines := []string{
		"my_counter:1|c|#host:localhost,environment:prod,endpoint:/:tenant?/oauth/ro",
		"my_gauge:10.1|g|#live",
		"my_set:1|s|#host:localhost",
		"my_timer:3|ms|@0.1|#live,host:localhost",
	}

	testTags := map[string]map[string]string{
		"my_counter": map[string]string{
			"host":        "localhost",
			"environment": "prod",
			"endpoint":    "/:tenant?/oauth/ro",
		},

		"my_gauge": map[string]string{
			"live": "",
		},

		"my_set": map[string]string{
			"host": "localhost",
		},

		"my_timer": map[string]string{
			"live": "",
			"host": "localhost",
		},
	}

	for _, line := range lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	sourceTags := map[string]map[string]string{
		"my_gauge":   tagsForItem(s.gauges),
		"my_counter": tagsForItem(s.counters),
		"my_set":     tagsForItem(s.sets),
		"my_timer":   tagsForItem(s.timings),
	}

	for statName, tags := range testTags {
		for k, v := range tags {
			otherValue := sourceTags[statName][k]
			if sourceTags[statName][k] != v {
				t.Errorf("Error with %s, tag %s: %s != %s", statName, k, v, otherValue)
			}
		}
	}
}

func tagsForItem(m interface{}) map[string]string {
	switch m.(type) {
	case map[string]cachedcounter:
		for _, v := range m.(map[string]cachedcounter) {
			return v.tags
		}
	case map[string]cachedgauge:
		for _, v := range m.(map[string]cachedgauge) {
			return v.tags
		}
	case map[string]cachedset:
		for _, v := range m.(map[string]cachedset) {
			return v.tags
		}
	case map[string]cachedtimings:
		for _, v := range m.(map[string]cachedtimings) {
			return v.tags
		}
	}
	return nil
}

// Test that statsd buckets are parsed to measurement names properly
func TestParseName(t *testing.T) {
	s := NewTestStatsd()

	tests := []struct {
		in_name  string
		out_name string
	}{
		{
			"foobar",
			"foobar",
		},
		{
			"foo.bar",
			"foo_bar",
		},
		{
			"foo.bar-baz",
			"foo_bar-baz",
		},
	}

	for _, test := range tests {
		name, _, _ := s.parseName(test.in_name)
		if name != test.out_name {
			t.Errorf("Expected: %s, got %s", test.out_name, name)
		}
	}

	// Test with separator == "."
	s.MetricSeparator = "."

	tests = []struct {
		in_name  string
		out_name string
	}{
		{
			"foobar",
			"foobar",
		},
		{
			"foo.bar",
			"foo.bar",
		},
		{
			"foo.bar-baz",
			"foo.bar-baz",
		},
	}

	for _, test := range tests {
		name, _, _ := s.parseName(test.in_name)
		if name != test.out_name {
			t.Errorf("Expected: %s, got %s", test.out_name, name)
		}
	}
}

// Test that measurements with the same name, but different tags, are treated
// as different outputs
func TestParse_MeasurementsWithSameName(t *testing.T) {
	s := NewTestStatsd()

	// Test that counters work
	valid_lines := []string{
		"test.counter,host=localhost:1|c",
		"test.counter,host=localhost,region=west:1|c",
	}

	for _, line := range valid_lines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	if len(s.counters) != 2 {
		t.Errorf("Expected 2 separate measurements, found %d", len(s.counters))
	}
}

// Test that measurements with multiple bits, are treated as different outputs
// but are equal to their single-measurement representation
func TestParse_MeasurementsWithMultipleValues(t *testing.T) {
	single_lines := []string{
		"valid.multiple:0|ms|@0.1",
		"valid.multiple:0|ms|",
		"valid.multiple:1|ms",
		"valid.multiple.duplicate:1|c",
		"valid.multiple.duplicate:1|c",
		"valid.multiple.duplicate:2|c",
		"valid.multiple.duplicate:1|c",
		"valid.multiple.duplicate:1|h",
		"valid.multiple.duplicate:1|h",
		"valid.multiple.duplicate:2|h",
		"valid.multiple.duplicate:1|h",
		"valid.multiple.duplicate:1|s",
		"valid.multiple.duplicate:1|s",
		"valid.multiple.duplicate:2|s",
		"valid.multiple.duplicate:1|s",
		"valid.multiple.duplicate:1|g",
		"valid.multiple.duplicate:1|g",
		"valid.multiple.duplicate:2|g",
		"valid.multiple.duplicate:1|g",
		"valid.multiple.mixed:1|c",
		"valid.multiple.mixed:1|ms",
		"valid.multiple.mixed:2|s",
		"valid.multiple.mixed:1|g",
	}

	multiple_lines := []string{
		"valid.multiple:0|ms|@0.1:0|ms|:1|ms",
		"valid.multiple.duplicate:1|c:1|c:2|c:1|c",
		"valid.multiple.duplicate:1|h:1|h:2|h:1|h",
		"valid.multiple.duplicate:1|s:1|s:2|s:1|s",
		"valid.multiple.duplicate:1|g:1|g:2|g:1|g",
		"valid.multiple.mixed:1|c:1|ms:2|s:1|g",
	}

	s_single := NewTestStatsd()
	s_multiple := NewTestStatsd()

	for _, line := range single_lines {
		err := s_single.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	for _, line := range multiple_lines {
		err := s_multiple.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}

	if len(s_single.timings) != 3 {
		t.Errorf("Expected 3 measurement, found %d", len(s_single.timings))
	}

	if cachedtiming, ok := s_single.timings["metric_type=timingvalid_multiple"]; !ok {
		t.Errorf("Expected cached measurement with hash 'metric_type=timingvalid_multiple' not found")
	} else {
		if cachedtiming.name != "valid_multiple" {
			t.Errorf("Expected the name to be 'valid_multiple', got %s", cachedtiming.name)
		}

		// A 0 at samplerate 0.1 will add 10 values of 0,
		// A 0 with invalid samplerate will add a single 0,
		// plus the last bit of value 1
		// which adds up to 12 individual datapoints to be cached
		if cachedtiming.fields[defaultFieldName].n != 12 {
			t.Errorf("Expected 11 additions, got %d", cachedtiming.fields[defaultFieldName].n)
		}

		if cachedtiming.fields[defaultFieldName].upper != 1 {
			t.Errorf("Expected max input to be 1, got %f", cachedtiming.fields[defaultFieldName].upper)
		}
	}

	// test if s_single and s_multiple did compute the same stats for valid.multiple.duplicate
	if err := test_validate_set("valid_multiple_duplicate", 2, s_single.sets); err != nil {
		t.Error(err.Error())
	}

	if err := test_validate_set("valid_multiple_duplicate", 2, s_multiple.sets); err != nil {
		t.Error(err.Error())
	}

	if err := test_validate_counter("valid_multiple_duplicate", 5, s_single.counters); err != nil {
		t.Error(err.Error())
	}

	if err := test_validate_counter("valid_multiple_duplicate", 5, s_multiple.counters); err != nil {
		t.Error(err.Error())
	}

	if err := test_validate_gauge("valid_multiple_duplicate", 1, s_single.gauges); err != nil {
		t.Error(err.Error())
	}

	if err := test_validate_gauge("valid_multiple_duplicate", 1, s_multiple.gauges); err != nil {
		t.Error(err.Error())
	}

	// test if s_single and s_multiple did compute the same stats for valid.multiple.mixed
	if err := test_validate_set("valid_multiple_mixed", 1, s_single.sets); err != nil {
		t.Error(err.Error())
	}

	if err := test_validate_set("valid_multiple_mixed", 1, s_multiple.sets); err != nil {
		t.Error(err.Error())
	}

	if err := test_validate_counter("valid_multiple_mixed", 1, s_single.counters); err != nil {
		t.Error(err.Error())
	}

	if err := test_validate_counter("valid_multiple_mixed", 1, s_multiple.counters); err != nil {
		t.Error(err.Error())
	}

	if err := test_validate_gauge("valid_multiple_mixed", 1, s_single.gauges); err != nil {
		t.Error(err.Error())
	}

	if err := test_validate_gauge("valid_multiple_mixed", 1, s_multiple.gauges); err != nil {
		t.Error(err.Error())
	}
}

// Tests low-level functionality of timings when multiple fields is enabled
// and a measurement template has been defined which can parse field names
func TestParse_Timings_MultipleFieldsWithTemplate(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{"measurement.field"}
	s.Percentiles = []int{90}
	acc := &testutil.Accumulator{}

	validLines := []string{
		"test_timing.success:1|ms",
		"test_timing.success:11|ms",
		"test_timing.success:1|ms",
		"test_timing.success:1|ms",
		"test_timing.success:1|ms",
		"test_timing.error:2|ms",
		"test_timing.error:22|ms",
		"test_timing.error:2|ms",
		"test_timing.error:2|ms",
		"test_timing.error:2|ms",
	}

	for _, line := range validLines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}
	s.Gather(acc)

	valid := map[string]interface{}{
		"success_90_percentile": float64(11),
		"success_count":         int64(5),
		"success_lower":         float64(1),
		"success_mean":          float64(3),
		"success_stddev":        float64(4),
		"success_upper":         float64(11),

		"error_90_percentile": float64(22),
		"error_count":         int64(5),
		"error_lower":         float64(2),
		"error_mean":          float64(6),
		"error_stddev":        float64(8),
		"error_upper":         float64(22),
	}

	acc.AssertContainsFields(t, "test_timing", valid)
}

// Tests low-level functionality of timings when multiple fields is enabled
// but a measurement template hasn't been defined so we can't parse field names
// In this case the behaviour should be the same as normal behaviour
func TestParse_Timings_MultipleFieldsWithoutTemplate(t *testing.T) {
	s := NewTestStatsd()
	s.Templates = []string{}
	s.Percentiles = []int{90}
	acc := &testutil.Accumulator{}

	validLines := []string{
		"test_timing.success:1|ms",
		"test_timing.success:11|ms",
		"test_timing.success:1|ms",
		"test_timing.success:1|ms",
		"test_timing.success:1|ms",
		"test_timing.error:2|ms",
		"test_timing.error:22|ms",
		"test_timing.error:2|ms",
		"test_timing.error:2|ms",
		"test_timing.error:2|ms",
	}

	for _, line := range validLines {
		err := s.parseStatsdLine(line)
		if err != nil {
			t.Errorf("Parsing line %s should not have resulted in an error\n", line)
		}
	}
	s.Gather(acc)

	expectedSuccess := map[string]interface{}{
		"90_percentile": float64(11),
		"count":         int64(5),
		"lower":         float64(1),
		"mean":          float64(3),
		"stddev":        float64(4),
		"upper":         float64(11),
	}
	expectedError := map[string]interface{}{
		"90_percentile": float64(22),
		"count":         int64(5),
		"lower":         float64(2),
		"mean":          float64(6),
		"stddev":        float64(8),
		"upper":         float64(22),
	}

	acc.AssertContainsFields(t, "test_timing_success", expectedSuccess)
	acc.AssertContainsFields(t, "test_timing_error", expectedError)
}

func BenchmarkParse(b *testing.B) {
	s := NewTestStatsd()
	validLines := []string{
		"test.timing.success:1|ms",
		"test.timing.success:11|ms",
		"test.timing.success:1|ms",
		"test.timing.success:1|ms",
		"test.timing.success:1|ms",
		"test.timing.error:2|ms",
		"test.timing.error:22|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
	}
	for n := 0; n < b.N; n++ {
		for _, line := range validLines {
			err := s.parseStatsdLine(line)
			if err != nil {
				b.Errorf("Parsing line %s should not have resulted in an error\n", line)
			}
		}
	}
}

func BenchmarkParseWithTemplate(b *testing.B) {
	s := NewTestStatsd()
	s.Templates = []string{"measurement.measurement.field"}
	validLines := []string{
		"test.timing.success:1|ms",
		"test.timing.success:11|ms",
		"test.timing.success:1|ms",
		"test.timing.success:1|ms",
		"test.timing.success:1|ms",
		"test.timing.error:2|ms",
		"test.timing.error:22|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
	}
	for n := 0; n < b.N; n++ {
		for _, line := range validLines {
			err := s.parseStatsdLine(line)
			if err != nil {
				b.Errorf("Parsing line %s should not have resulted in an error\n", line)
			}
		}
	}
}

func BenchmarkParseWithTemplateAndFilter(b *testing.B) {
	s := NewTestStatsd()
	s.Templates = []string{"cpu* measurement.measurement.field"}
	validLines := []string{
		"test.timing.success:1|ms",
		"test.timing.success:11|ms",
		"test.timing.success:1|ms",
		"cpu.timing.success:1|ms",
		"cpu.timing.success:1|ms",
		"cpu.timing.error:2|ms",
		"cpu.timing.error:22|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
	}
	for n := 0; n < b.N; n++ {
		for _, line := range validLines {
			err := s.parseStatsdLine(line)
			if err != nil {
				b.Errorf("Parsing line %s should not have resulted in an error\n", line)
			}
		}
	}
}

func BenchmarkParseWith2TemplatesAndFilter(b *testing.B) {
	s := NewTestStatsd()
	s.Templates = []string{
		"cpu1* measurement.measurement.field",
		"cpu2* measurement.measurement.field",
	}
	validLines := []string{
		"test.timing.success:1|ms",
		"test.timing.success:11|ms",
		"test.timing.success:1|ms",
		"cpu1.timing.success:1|ms",
		"cpu1.timing.success:1|ms",
		"cpu2.timing.error:2|ms",
		"cpu2.timing.error:22|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
		"test.timing.error:2|ms",
	}
	for n := 0; n < b.N; n++ {
		for _, line := range validLines {
			err := s.parseStatsdLine(line)
			if err != nil {
				b.Errorf("Parsing line %s should not have resulted in an error\n", line)
			}
		}
	}
}

func BenchmarkParseWith2Templates3TagsAndFilter(b *testing.B) {
	s := NewTestStatsd()
	s.Templates = []string{
		"cpu1* measurement.measurement.region.city.rack.field",
		"cpu2* measurement.measurement.region.city.rack.field",
	}
	validLines := []string{
		"test.timing.us-east.nyc.rack01.success:1|ms",
		"test.timing.us-east.nyc.rack01.success:11|ms",
		"test.timing.us-west.sf.rack01.success:1|ms",
		"cpu1.timing.us-west.sf.rack01.success:1|ms",
		"cpu1.timing.us-east.nyc.rack01.success:1|ms",
		"cpu2.timing.us-east.nyc.rack01.error:2|ms",
		"cpu2.timing.us-west.sf.rack01.error:22|ms",
		"test.timing.us-west.sf.rack01.error:2|ms",
		"test.timing.us-west.sf.rack01.error:2|ms",
		"test.timing.us-east.nyc.rack01.error:2|ms",
	}
	for n := 0; n < b.N; n++ {
		for _, line := range validLines {
			err := s.parseStatsdLine(line)
			if err != nil {
				b.Errorf("Parsing line %s should not have resulted in an error\n", line)
			}
		}
	}
}

func TestParse_Timings_Delete(t *testing.T) {
	s := NewTestStatsd()
	s.DeleteTimings = true
	fakeacc := &testutil.Accumulator{}
	var err error

	line := "timing:100|ms"
	err = s.parseStatsdLine(line)
	if err != nil {
		t.Errorf("Parsing line %s should not have resulted in an error\n", line)
	}

	if len(s.timings) != 1 {
		t.Errorf("Should be 1 timing, found %d", len(s.timings))
	}

	s.Gather(fakeacc)

	if len(s.timings) != 0 {
		t.Errorf("All timings should have been deleted, found %d", len(s.timings))
	}
}

// Tests the delete_gauges option
func TestParse_Gauges_Delete(t *testing.T) {
	s := NewTestStatsd()
	s.DeleteGauges = true
	fakeacc := &testutil.Accumulator{}
	var err error

	line := "current.users:100|g"
	err = s.parseStatsdLine(line)
	if err != nil {
		t.Errorf("Parsing line %s should not have resulted in an error\n", line)
	}

	err = test_validate_gauge("current_users", 100, s.gauges)
	if err != nil {
		t.Error(err.Error())
	}

	s.Gather(fakeacc)

	err = test_validate_gauge("current_users", 100, s.gauges)
	if err == nil {
		t.Error("current_users_gauge metric should have been deleted")
	}
}

// Tests the delete_sets option
func TestParse_Sets_Delete(t *testing.T) {
	s := NewTestStatsd()
	s.DeleteSets = true
	fakeacc := &testutil.Accumulator{}
	var err error

	line := "unique.user.ids:100|s"
	err = s.parseStatsdLine(line)
	if err != nil {
		t.Errorf("Parsing line %s should not have resulted in an error\n", line)
	}

	err = test_validate_set("unique_user_ids", 1, s.sets)
	if err != nil {
		t.Error(err.Error())
	}

	s.Gather(fakeacc)

	err = test_validate_set("unique_user_ids", 1, s.sets)
	if err == nil {
		t.Error("unique_user_ids_set metric should have been deleted")
	}
}

// Tests the delete_counters option
func TestParse_Counters_Delete(t *testing.T) {
	s := NewTestStatsd()
	s.DeleteCounters = true
	fakeacc := &testutil.Accumulator{}
	var err error

	line := "total.users:100|c"
	err = s.parseStatsdLine(line)
	if err != nil {
		t.Errorf("Parsing line %s should not have resulted in an error\n", line)
	}

	err = test_validate_counter("total_users", 100, s.counters)
	if err != nil {
		t.Error(err.Error())
	}

	s.Gather(fakeacc)

	err = test_validate_counter("total_users", 100, s.counters)
	if err == nil {
		t.Error("total_users_counter metric should have been deleted")
	}
}

func TestParseKeyValue(t *testing.T) {
	k, v := parseKeyValue("foo=bar")
	if k != "foo" {
		t.Errorf("Expected %s, got %s", "foo", k)
	}
	if v != "bar" {
		t.Errorf("Expected %s, got %s", "bar", v)
	}

	k2, v2 := parseKeyValue("baz")
	if k2 != "" {
		t.Errorf("Expected %s, got %s", "", k2)
	}
	if v2 != "baz" {
		t.Errorf("Expected %s, got %s", "baz", v2)
	}
}

// Test utility functions

func test_validate_set(
	name string,
	value int64,
	cache map[string]cachedset,
	field ...string,
) error {
	var f string
	if len(field) > 0 {
		f = field[0]
	} else {
		f = "value"
	}
	var metric cachedset
	var found bool
	for _, v := range cache {
		if v.name == name {
			metric = v
			found = true
			break
		}
	}
	if !found {
		return errors.New(fmt.Sprintf("Test Error: Metric name %s not found\n", name))
	}

	if value != int64(len(metric.fields[f])) {
		return errors.New(fmt.Sprintf("Measurement: %s, expected %d, actual %d\n",
			name, value, len(metric.fields[f])))
	}
	return nil
}

func test_validate_counter(
	name string,
	valueExpected int64,
	cache map[string]cachedcounter,
	field ...string,
) error {
	var f string
	if len(field) > 0 {
		f = field[0]
	} else {
		f = "value"
	}
	var valueActual int64
	var found bool
	for _, v := range cache {
		if v.name == name {
			valueActual = v.fields[f].(int64)
			found = true
			break
		}
	}
	if !found {
		return errors.New(fmt.Sprintf("Test Error: Metric name %s not found\n", name))
	}

	if valueExpected != valueActual {
		return errors.New(fmt.Sprintf("Measurement: %s, expected %d, actual %d\n",
			name, valueExpected, valueActual))
	}
	return nil
}

func test_validate_gauge(
	name string,
	valueExpected float64,
	cache map[string]cachedgauge,
	field ...string,
) error {
	var f string
	if len(field) > 0 {
		f = field[0]
	} else {
		f = "value"
	}
	var valueActual float64
	var found bool
	for _, v := range cache {
		if v.name == name {
			valueActual = v.fields[f].(float64)
			found = true
			break
		}
	}
	if !found {
		return errors.New(fmt.Sprintf("Test Error: Metric name %s not found\n", name))
	}

	if valueExpected != valueActual {
		return errors.New(fmt.Sprintf("Measurement: %s, expected %f, actual %f\n",
			name, valueExpected, valueActual))
	}
	return nil
}
