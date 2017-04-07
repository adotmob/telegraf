package druid

import (
	"encoding/json"

	"github.com/influxdata/telegraf"
)

type DruidSerializer struct {
}

func (s *DruidSerializer) Serialize(metric telegraf.Metric) ([]byte, error) {
	res := make([]byte,0)
	for key, value := range metric.Fields() {
		m := make(map[string]interface{})
		m["origin"] = metric.Name()
		m["timestamp"] = metric.UnixNano() / 1000000

		m["name"] = key
		for keyTag, valueTag := range metric.Tags() {
			m[keyTag] =valueTag
		}
		m["value"] = value
		serialized, err := json.Marshal(m)
		if err != nil {
			return []byte{}, err
		}
		serialized = append(serialized, '\n')
		res = append(res, serialized...)
	}
	return res, nil
}
