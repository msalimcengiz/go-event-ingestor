// Package event defines the ingested event model and its validation rules.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	maxTypeLen    = 64
	maxUserIDLen  = 128
	maxPayloadLen = 16 << 10
	maxClockSkew  = 5 * time.Minute
)

type Event struct {
	ID         uuid.UUID       `json:"id"`
	Type       string          `json:"type"`
	UserID     string          `json:"user_id"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

var ErrInvalid = errors.New("invalid event")

// Normalize fills server-side defaults and validates the event.
// Clients may send their own ID so retries stay idempotent.
func (e *Event) Normalize(now time.Time) error {
	if e.ID == uuid.Nil {
		e.ID = uuid.New()
	}
	if e.OccurredAt.IsZero() {
		e.OccurredAt = now
	}
	switch {
	case e.Type == "" || len(e.Type) > maxTypeLen:
		return fmt.Errorf("%w: type must be 1-%d chars", ErrInvalid, maxTypeLen)
	case e.UserID == "" || len(e.UserID) > maxUserIDLen:
		return fmt.Errorf("%w: user_id must be 1-%d chars", ErrInvalid, maxUserIDLen)
	case e.OccurredAt.After(now.Add(maxClockSkew)):
		return fmt.Errorf("%w: occurred_at is in the future", ErrInvalid)
	case len(e.Payload) > maxPayloadLen:
		return fmt.Errorf("%w: payload exceeds %d bytes", ErrInvalid, maxPayloadLen)
	}
	if len(e.Payload) == 0 {
		e.Payload = json.RawMessage("{}")
	} else if !json.Valid(e.Payload) || e.Payload[0] != '{' {
		return fmt.Errorf("%w: payload must be a JSON object", ErrInvalid)
	} else if bytes.Contains(e.Payload, []byte(`\u0000`)) {
		// Valid JSON, but Postgres jsonb cannot store NUL characters.
		return fmt.Errorf("%w: payload must not contain \\u0000", ErrInvalid)
	}
	e.OccurredAt = e.OccurredAt.UTC()
	return nil
}
