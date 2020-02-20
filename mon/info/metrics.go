package info

//Metrics represents info metrics
type Metrics struct {
	index map[string]*Metric
	Items []*Metric `json:",omitempty"`
}

//GetOrCreate returns metrics for supplied key
func (m *Metrics) GetOrCreate(key string) *Metric {
	if len(m.index) == 0 {
		m.index = make(map[string]*Metric)
		m.Items = make([]*Metric, 0)
	}
	result, ok := m.index[key]
	if !ok {
		result = NewMetric()
		result.Key = key
		m.index[key] = result
		m.Items = append(m.Items, result)
	}
	return result
}
