package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/msalimcengiz/go-event-ingestor/internal/event"
)

func newServer(sink Sink) *Server {
	return &Server{
		Sink:         sink,
		Mode:         "test",
		MaxBodyBytes: 1 << 16,
		Ready:        func(context.Context) error { return nil },
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:          func() time.Time { return time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC) },
	}
}

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAcceptsSingleAndBatch(t *testing.T) {
	var got []event.Event
	h := newServer(func(_ context.Context, ev []event.Event) error {
		got = append(got, ev...)
		return nil
	}).Routes()

	if rec := post(t, h, `{"type":"click","user_id":"u1"}`); rec.Code != http.StatusAccepted {
		t.Fatalf("single: got %d %s", rec.Code, rec.Body)
	}
	rec := post(t, h, `[{"type":"a","user_id":"u1"},{"type":"b","user_id":"u2","payload":{"k":"v"}}]`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("batch: got %d %s", rec.Code, rec.Body)
	}
	var resp acceptedResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Accepted != 2 || len(resp.IDs) != 2 {
		t.Fatalf("unexpected response %+v", resp)
	}
	if len(got) != 3 {
		t.Fatalf("sink received %d events, want 3", len(got))
	}
}

func TestRejectsInvalidEventWithIndex(t *testing.T) {
	called := false
	h := newServer(func(context.Context, []event.Event) error { called = true; return nil }).Routes()

	rec := post(t, h, `[{"type":"a","user_id":"u1"},{"type":"","user_id":"u2"}]`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("got %d, want 422", rec.Code)
	}
	var resp errorResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Index == nil || *resp.Index != 1 {
		t.Fatalf("want index 1, got %+v", resp)
	}
	if called {
		t.Fatal("sink must not be called when any event is invalid")
	}
}

func TestBadJSONAndSizeLimits(t *testing.T) {
	h := newServer(func(context.Context, []event.Event) error { return nil }).Routes()

	if rec := post(t, h, `{not json`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json: got %d", rec.Code)
	}
	if rec := post(t, h, `[]`); rec.Code != http.StatusBadRequest {
		t.Fatalf("empty array: got %d", rec.Code)
	}
	big := `{"type":"a","user_id":"u1","payload":{"x":"` + strings.Repeat("y", 1<<17) + `"}}`
	if rec := post(t, h, big); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: got %d", rec.Code)
	}
}

func TestSinkFailureReturns503(t *testing.T) {
	h := newServer(func(context.Context, []event.Event) error { return errors.New("redis down") }).Routes()
	if rec := post(t, h, `{"type":"a","user_id":"u1"}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", rec.Code)
	}
}

func TestAuthentication(t *testing.T) {
	s := newServer(func(context.Context, []event.Event) error { return nil })
	s.APIKeys = []string{"key-a", "key-b"}
	h := s.Routes()

	for _, tc := range []struct {
		header string
		want   int
	}{
		{"", http.StatusUnauthorized},
		{"Bearer wrong", http.StatusUnauthorized},
		{"key-b", http.StatusUnauthorized},
		{"Bearer key-b", http.StatusAccepted},
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(`{"type":"a","user_id":"u1"}`))
		if tc.header != "" {
			req.Header.Set("Authorization", tc.header)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("header %q: got %d want %d", tc.header, rec.Code, tc.want)
		}
	}
}

func TestBackpressureReturns503WithRetryAfter(t *testing.T) {
	called := false
	s := newServer(func(context.Context, []event.Event) error { called = true; return nil })
	s.Admit = func() error { return ErrBackpressure }
	rec := post(t, s.Routes(), `{"type":"a","user_id":"u1"}`)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("got %d, Retry-After=%q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if called {
		t.Fatal("events must not be accepted under backpressure")
	}
}

func TestReadinessFailsWhileDraining(t *testing.T) {
	s := newServer(func(context.Context, []event.Event) error { return nil })
	s.Draining = &atomic.Bool{}
	s.Draining.Store(true)
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d", rec.Code)
	}
}

func TestGetEvent(t *testing.T) {
	stored := event.Event{ID: uuid.New(), Type: "click", UserID: "u1"}
	s := newServer(func(context.Context, []event.Event) error { return nil })
	s.Lookup = func(_ context.Context, id uuid.UUID) (*event.Event, error) {
		if id == stored.ID {
			return &stored, nil
		}
		return nil, nil
	}
	h := s.Routes()
	get := func(path string) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}
	if c := get("/v1/events/" + stored.ID.String()); c != http.StatusOK {
		t.Fatalf("stored: %d", c)
	}
	if c := get("/v1/events/" + uuid.NewString()); c != http.StatusNotFound {
		t.Fatalf("missing: %d", c)
	}
	if c := get("/v1/events/not-a-uuid"); c != http.StatusBadRequest {
		t.Fatalf("bad id: %d", c)
	}
}
