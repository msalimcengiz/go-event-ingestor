// Package stream buffers events in a Redis Stream and reads them back through a consumer group.
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/msalimcengiz/go-event-ingestor/internal/event"
)

const dataField = "d"

type Message struct {
	StreamID string
	Event    event.Event
	// Raw is the stored payload, kept so a message can be dead-lettered byte-for-byte.
	Raw string
}

// Dead is a message on its way to the dead-letter stream.
type Dead struct {
	StreamID string
	Raw      string
	Reason   string
}

type Redis struct {
	client *redis.Client
	stream string
	group  string
}

func NewRedis(client *redis.Client, stream, group string) *Redis {
	return &Redis{client: client, stream: stream, group: group}
}

func (r *Redis) Name() string { return r.stream }

func (r *Redis) DeadLetterStream() string { return r.stream + ":dead" }

// Publish appends events with one pipelined round trip.
//
// There is deliberately no MAXLEN here: capping on write would silently drop entries
// the workers have not read yet whenever they fall behind. The stream is bounded by
// Trim (which only removes acknowledged entries) and by API backpressure instead.
func (r *Redis) Publish(ctx context.Context, events []event.Event) error {
	pipe := r.client.Pipeline()
	for _, e := range events {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: r.stream, Values: map[string]any{dataField: b}})
	}
	_, err := pipe.Exec(ctx)
	return err
}

// EnsureGroup creates the consumer group (and the stream) if they do not exist yet.
func (r *Redis) EnsureGroup(ctx context.Context) error {
	err := r.client.XGroupCreateMkStream(ctx, r.stream, r.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Read blocks up to block for new messages. Undecodable messages are returned as dead letters.
func (r *Redis) Read(ctx context.Context, consumer string, count int64, block time.Duration) ([]Message, []Dead, error) {
	res, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    r.group,
		Consumer: consumer,
		Streams:  []string{r.stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var msgs []Message
	var dead []Dead
	for _, s := range res {
		m, d := decode(s.Messages)
		msgs = append(msgs, m...)
		dead = append(dead, d...)
	}
	return msgs, dead, nil
}

// Reclaim takes over messages that have been pending longer than minIdle, typically
// because the consumer that read them crashed. Messages already delivered maxDeliveries
// times are returned as dead letters instead, so one message that keeps failing cannot
// be retried forever.
func (r *Redis) Reclaim(ctx context.Context, consumer string, minIdle time.Duration, count int64, maxDeliveries int64) ([]Message, []Dead, error) {
	pending, err := r.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: r.stream,
		Group:  r.group,
		Idle:   minIdle,
		Start:  "-",
		End:    "+",
		Count:  count,
	}).Result()
	if err != nil || len(pending) == 0 {
		return nil, nil, err
	}

	ids := make([]string, len(pending))
	exhausted := make(map[string]int64)
	for i, p := range pending {
		ids[i] = p.ID
		if p.RetryCount >= maxDeliveries {
			exhausted[p.ID] = p.RetryCount
		}
	}
	// XCLAIM re-checks idle time atomically, so two workers reclaiming at once never
	// both get the same message.
	claimed, err := r.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   r.stream,
		Group:    r.group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		return nil, nil, err
	}

	var live []redis.XMessage
	var dead []Dead
	for _, m := range claimed {
		if n, ok := exhausted[m.ID]; ok {
			raw, _ := m.Values[dataField].(string)
			dead = append(dead, Dead{StreamID: m.ID, Raw: raw, Reason: fmt.Sprintf("delivered %d times without success", n)})
			continue
		}
		live = append(live, m)
	}
	msgs, undecodable := decode(live)
	return msgs, append(dead, undecodable...), nil
}

func (r *Redis) Ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	return r.client.XAck(ctx, r.stream, r.group, ids...).Err()
}

// DeadLetter moves messages to the dead-letter stream and acknowledges them in one
// MULTI/EXEC, so a message is never both dead-lettered and still pending.
func (r *Redis) DeadLetter(ctx context.Context, dead []Dead) error {
	if len(dead) == 0 {
		return nil
	}
	pipe := r.client.TxPipeline()
	ids := make([]string, 0, len(dead))
	for _, d := range dead {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: r.DeadLetterStream(),
			Values: map[string]any{"source_id": d.StreamID, "reason": d.Reason, dataField: d.Raw},
		})
		ids = append(ids, d.StreamID)
	}
	pipe.XAck(ctx, r.stream, r.group, ids...)
	_, err := pipe.Exec(ctx)
	return err
}

// Replay moves up to count messages from the dead-letter stream back into the main
// stream (after the underlying problem is fixed) and returns how many were moved.
func (r *Redis) Replay(ctx context.Context, count int64) (int, error) {
	msgs, err := r.client.XRangeN(ctx, r.DeadLetterStream(), "-", "+", count).Result()
	if err != nil {
		return 0, err
	}
	for _, m := range msgs {
		pipe := r.client.TxPipeline()
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: r.stream, Values: map[string]any{dataField: m.Values[dataField]}})
		pipe.XDel(ctx, r.DeadLetterStream(), m.ID)
		if _, err := pipe.Exec(ctx); err != nil {
			return 0, err
		}
	}
	return len(msgs), nil
}

// Backlog is what the consumer group still has to finish.
type Backlog struct {
	Lag     int64 // entries not yet delivered to any consumer
	Pending int64 // delivered but not acknowledged
	Length  int64 // entries kept in the stream
}

func (b Backlog) Total() int64 { return b.Lag + b.Pending }

func (r *Redis) Backlog(ctx context.Context) (Backlog, error) {
	var b Backlog
	groups, err := r.client.XInfoGroups(ctx, r.stream).Result()
	if err != nil {
		return b, err
	}
	for _, g := range groups {
		if g.Name == r.group {
			b.Lag, b.Pending = g.Lag, g.Pending
		}
	}
	b.Length, err = r.client.XLen(ctx, r.stream).Result()
	return b, err
}

// Trim deletes entries that the consumer group has fully processed: everything older
// than the oldest pending entry, or than the last delivered entry when nothing is
// pending. It never removes an entry that has not been acknowledged.
func (r *Redis) Trim(ctx context.Context) (int64, error) {
	groups, err := r.client.XInfoGroups(ctx, r.stream).Result()
	if err != nil {
		return 0, err
	}
	var lastDelivered string
	for _, g := range groups {
		if g.Name == r.group {
			lastDelivered = g.LastDeliveredID
		}
	}
	if lastDelivered == "" || lastDelivered == "0-0" {
		return 0, nil
	}
	minID := lastDelivered
	summary, err := r.client.XPending(ctx, r.stream, r.group).Result()
	if err != nil {
		return 0, err
	}
	if summary.Count > 0 {
		minID = summary.Lower
	}
	return r.client.XTrimMinIDApprox(ctx, r.stream, minID, 0).Result()
}

func decode(raw []redis.XMessage) ([]Message, []Dead) {
	var msgs []Message
	var dead []Dead
	for _, m := range raw {
		s, ok := m.Values[dataField].(string)
		var e event.Event
		if !ok || json.Unmarshal([]byte(s), &e) != nil {
			dead = append(dead, Dead{StreamID: m.ID, Raw: s, Reason: "undecodable message"})
			continue
		}
		msgs = append(msgs, Message{StreamID: m.ID, Event: e, Raw: s})
	}
	return msgs, dead
}
