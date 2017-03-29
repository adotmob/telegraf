package druid

import (
	"fmt"
	"github.com/influxdata/telegraf/metric"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSerializeMetricFloat(t *testing.T) {
	now := time.Now()
	tags := map[string]string{
		"cpu": "cpu0",
	}
	fields := map[string]interface{}{
		"usage_idle": float64(91.5),
	}
	m, err := metric.New("cpu", tags, fields, now)
	assert.NoError(t, err)

	s := DruidSerializer{}
	var buf []byte
	buf, err = s.Serialize(m)
	assert.NoError(t, err)
	expS := []byte(fmt.Sprintf(`{"cpu":"cpu0","name":"usage_idle","origin":"cpu","timestamp":%d,"value":91.5}`, now.Unix()) + "\n")
	assert.Equal(t, string(expS), string(buf))
}

func TestSerializeMetricInt(t *testing.T) {
	now := time.Now()
	tags := map[string]string{
		"cpu": "cpu0",
	}
	fields := map[string]interface{}{
		"usage_idle": int64(90),
	}
	m, err := metric.New("cpu", tags, fields, now)
	assert.NoError(t, err)

	s := DruidSerializer{}
	var buf []byte
	buf, err = s.Serialize(m)
	assert.NoError(t, err)

	expS := []byte(fmt.Sprintf(`{"cpu":"cpu0","name":"usage_idle","origin":"cpu","timestamp":%d,"value":90}`, now.Unix()) + "\n")
	assert.Equal(t, string(expS), string(buf))
}

func TestSerializeMultiFields(t *testing.T) {
	now := time.Now()
	tags := map[string]string{
		"cpu": "cpu0",
	}
	fields := map[string]interface{}{
		"usage_idle":  int64(90),
		"usage_total": 8559615,
	}
	m, err := metric.New("cpu", tags, fields, now)
	assert.NoError(t, err)

	s := DruidSerializer{}
	var buf []byte
	buf, err = s.Serialize(m)
	assert.NoError(t, err)

	expS := []byte(fmt.Sprintf(`{"cpu":"cpu0","name":"usage_idle","origin":"cpu","timestamp":%d,"value":90}`, now.Unix()) +
		"\n" +
		fmt.Sprintf(`{"cpu":"cpu0","name":"usage_total","origin":"cpu","timestamp":%d,"value":8559615}`, now.Unix()) +
		"\n")
	assert.Equal(t, string(expS), string(buf))
}

func TestSerializeMetricWithEscapes(t *testing.T) {
	now := time.Now()
	tags := map[string]string{
		"cpu tag": "cpu0",
	}
	fields := map[string]interface{}{
		"U,age=Idle": int64(90),
	}
	m, err := metric.New("My CPU", tags, fields, now)
	assert.NoError(t, err)

	s := DruidSerializer{}
	buf, err := s.Serialize(m)
	assert.NoError(t, err)

	expS := []byte(fmt.Sprintf(`{"cpu tag":"cpu0","name":"U,age=Idle","origin":"My CPU","timestamp":%d,"value":90}`, now.Unix()) + "\n")
	assert.Equal(t, string(expS), string(buf))
}
