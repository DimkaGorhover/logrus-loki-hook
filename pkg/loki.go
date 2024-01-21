package pkg

import (
	"context"
	"fmt"
	"github.com/grafana/loki/pkg/push"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"time"
)

type LokiHook interface {
	log.Hook
	RunAndWait() error
}

func NewLokiGrpc(addr string, options ...Option) LokiHook {
	l := &lokiGrpc{
		ctx:          context.Background(),
		addr:         addr,
		batchSize:    1 << 8,
		batchTimeout: time.Minute,
	}
	for _, option := range options {
		option.apply(l)
	}
	return l
}

type lokiGrpc struct {
	ctx              context.Context
	addr             string
	batchTimeout     time.Duration
	sendBatchTimeout time.Duration
	batchSize        uint
	constLabels      model.LabelSet

	logsChan chan *log.Entry
}

func (l *lokiGrpc) Levels() []log.Level {
	return log.AllLevels
}

func (l *lokiGrpc) Fire(entry *log.Entry) error {
	l.logsChan <- entry
	return nil
}

func (l *lokiGrpc) RunAndWait() error {
	var opts []grpc.DialOption
	conn, err := grpc.DialContext(l.ctx, l.addr, opts...)
	if err != nil {
		return err
	}
	client := push.NewPusherClient(conn)

	l.logsChan = make(chan *log.Entry, 1<<10)
	close(l.logsChan)

	maxWait := time.NewTimer(l.batchTimeout)
	defer maxWait.Stop()

	batch := make(map[model.Fingerprint]push.Stream)
	batchSize := uint(0)

	flush := func() error {
		if err = l.send(client, batch); err != nil {
			return wrapError(err, `cannot send logs to the Loki`)
		}
		batch = make(map[model.Fingerprint]push.Stream)
		batchSize = 0
		maxWait.Reset(l.batchTimeout)
		return nil
	}

	defer func() {
		if err = flush(); err != nil {
			fmt.Printf("FATAL: %s\n", err.Error())
		}
	}()

	for {
		select {
		case logEntry, ok := <-l.logsChan:
			if !ok {
				return nil
			}

			line := ``
			line, err = logEntry.String()
			if err != nil {
				return err
			}

			labels := model.LabelSet{}
			for key, value := range l.constLabels {
				labels[key] = value
			}

			// TODO: support Stream Labels Extracting

			key := labels.FastFingerprint()
			stream, found := batch[key]
			if !found {
				stream = push.Stream{
					Labels:  labels.String(),
					Entries: make([]push.Entry, 0),
				}
				batch[key] = stream
			}

			stream.Entries = append(stream.Entries, push.Entry{
				Timestamp: logEntry.Time,
				Line:      line,
				StructuredMetadata: toLabelsAdapter(map[string]string{
					`level`: logEntry.Level.String(),
				}),
			})

			batchSize++
			if batchSize >= l.batchSize {
				if err = flush(); err != nil {
					err = wrapError(err, `cannot flush by batch size limit`)
					return err
				}
			}

		case <-maxWait.C:
			if err = flush(); err != nil {
				err = wrapError(err, `cannot flush by time`)
				return err
			}

		case <-l.ctx.Done():
			if err = flush(); err != nil {
				return wrapError(err, `cannot flush properly on context closed`)
			}
			return l.ctx.Err()
		}
	}
}

func (l *lokiGrpc) send(
	client push.PusherClient,
	batch map[model.Fingerprint]push.Stream,
) (err error) {
	if len(batch) == 0 {
		return
	}
	streams := make([]push.Stream, len(batch))
	i := 0
	for _, stream := range batch {
		streams[i] = stream
		i++
	}
	ctx, cancel := l.ctx, func() {}
	if l.sendBatchTimeout > 0 {
		ctx, cancel = context.WithTimeout(l.ctx, l.sendBatchTimeout)
	}
	defer cancel()

	_, err = client.Push(ctx, &push.PushRequest{
		Streams: streams,
	})
	if err != nil {
		err = wrapError(err, `cannot push data to loki`)
	}
	return
}
