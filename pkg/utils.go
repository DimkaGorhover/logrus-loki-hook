package pkg

import (
	. "github.com/grafana/loki/pkg/push"
)

func toLabelsAdapter(m map[string]string) LabelsAdapter {
	la := make(LabelsAdapter, len(m))
	i := 0
	for key, value := range m {
		la[i] = LabelAdapter{
			Name:  key,
			Value: value,
		}
		i++
	}
	return la
}
