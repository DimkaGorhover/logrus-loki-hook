package pkg

import (
	"context"
	. "github.com/prometheus/common/model"
	"time"
)

type Option interface {
	apply(l *lokiGrpc)
}

func WithContext(ctx context.Context) Option {
	return optionFunc(func(l *lokiGrpc) {
		if ctx != nil {
			l.ctx = ctx
		}
	})
}

func WithLabels(labels map[string]string) Option {
	return optionFunc(func(l *lokiGrpc) {
		l.constLabels = make(LabelSet, len(labels))
		if labels != nil {
			for key, value := range labels {
				l.constLabels[LabelName(key)] = LabelValue(value)
			}
		}
	})
}

func WithBatchTimeout(batchTimeout time.Duration) Option {
	return optionFunc(func(l *lokiGrpc) {
		l.batchTimeout = batchTimeout
	})
}

func WithBatchSize(batchSize uint) Option {
	return optionFunc(func(l *lokiGrpc) {
		l.batchSize = batchSize
	})
}

func WithSendBatchTimeout(sendBatchTimeout time.Duration) Option {
	return optionFunc(func(l *lokiGrpc) {
		l.sendBatchTimeout = sendBatchTimeout
	})
}

type optionFunc func(l *lokiGrpc)

func (o optionFunc) apply(l *lokiGrpc) {
	o(l)
}
