package event

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNormalize(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		in      Event
		wantErr bool
	}{
		{name: "minimal valid", in: Event{Type: "page_view", UserID: "u1"}},
		{name: "with payload", in: Event{Type: "click", UserID: "u1", Payload: json.RawMessage(`{"x":1}`)}},
		{name: "missing type", in: Event{UserID: "u1"}, wantErr: true},
		{name: "missing user", in: Event{Type: "click"}, wantErr: true},
		{name: "type too long", in: Event{Type: strings.Repeat("a", 65), UserID: "u1"}, wantErr: true},
		{name: "future timestamp", in: Event{Type: "click", UserID: "u1", OccurredAt: now.Add(time.Hour)}, wantErr: true},
		{name: "payload is array", in: Event{Type: "click", UserID: "u1", Payload: json.RawMessage(`[1,2]`)}, wantErr: true},
		{name: "payload too big", in: Event{Type: "click", UserID: "u1", Payload: json.RawMessage(`{"a":"` + strings.Repeat("x", 17<<10) + `"}`)}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := tt.in
			err := e.Normalize(now)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("want ErrInvalid, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if e.ID == uuid.Nil {
				t.Error("ID was not generated")
			}
			if e.OccurredAt.IsZero() {
				t.Error("OccurredAt was not defaulted")
			}
			if len(e.Payload) == 0 {
				t.Error("Payload was not defaulted")
			}
		})
	}
}

func TestNormalizeKeepsClientID(t *testing.T) {
	id := uuid.New()
	e := Event{ID: id, Type: "click", UserID: "u1"}
	if err := e.Normalize(time.Now()); err != nil {
		t.Fatal(err)
	}
	if e.ID != id {
		t.Fatalf("client ID overwritten: got %s want %s", e.ID, id)
	}
}
