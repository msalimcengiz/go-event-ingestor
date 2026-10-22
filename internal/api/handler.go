// Package api exposes the HTTP ingestion endpoint.
package api

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/msalimcengiz/go-event-ingestor/internal/event"
	"github.com/msalimcengiz/go-event-ingestor/internal/metrics"
)

const maxEventsPerRequest = 1000

// Sink receives validated events. In buffered mode it publishes to Redis,
// in direct mode it writes to Postgres.
type Sink func(ctx context.Context, events []event.Event) error

// ErrBackpressure tells the client to slow down; the workers are behind.
var ErrBackpressure = errors.New("backlog over limit")

type Server struct {
	Sink         Sink
	Mode         string
	MaxBodyBytes int64
	Ready        func(ctx context.Context) error
	// Admit is checked before accepting events; ErrBackpressure turns into 503 + Retry-After.
	Admit func() error
	// APIKeys accepted as bearer tokens. Empty disables authentication.
	APIKeys  []string
	Draining *atomic.Bool
	// Lookup returns a stored event, or nil if it is not (yet) in Postgres.
	Lookup func(ctx context.Context, id uuid.UUID) (*event.Event, error)
	Logger *slog.Logger
	Now    func() time.Time
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/events", s.instrument("events", s.authenticate(http.HandlerFunc(s.handleEvents))))
	mux.Handle("GET /v1/events/{id}", s.instrument("event", s.authenticate(http.HandlerFunc(s.handleGet))))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

type acceptedResponse struct {
	Accepted int         `json:"accepted"`
	IDs      []uuid.UUID `json:"ids"`
}

type errorResponse struct {
	Error string `json:"error"`
	Index *int   `json:"index,omitempty"`
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(s.APIKeys) > 0 {
			token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || !validKey(token, s.APIKeys) {
				writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "missing or invalid API key"})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func validKey(token string, keys []string) bool {
	match := 0
	for _, k := range keys {
		// Compare against every key in constant time so timing reveals nothing.
		match |= subtle.ConstantTimeCompare([]byte(token), []byte(k))
	}
	return match == 1
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if s.Admit != nil {
		if err := s.Admit(); err != nil {
			metrics.BackpressureRejections.Inc()
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "ingestion is behind, retry with the same ids"})
			return
		}
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.MaxBodyBytes)
	events, err := decodeEvents(r)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			metrics.EventsRejected.WithLabelValues("too_large").Inc()
			writeJSON(w, http.StatusRequestEntityTooLarge, errorResponse{Error: "request body too large"})
			return
		}
		metrics.EventsRejected.WithLabelValues("bad_json").Inc()
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	if len(events) == 0 || len(events) > maxEventsPerRequest {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: fmt.Sprintf("send 1-%d events per request", maxEventsPerRequest)})
		return
	}

	now := s.Now()
	ids := make([]uuid.UUID, len(events))
	for i := range events {
		if err := events[i].Normalize(now); err != nil {
			metrics.EventsRejected.WithLabelValues("invalid").Add(float64(len(events)))
			idx := i
			writeJSON(w, http.StatusUnprocessableEntity, errorResponse{Error: err.Error(), Index: &idx})
			return
		}
		ids[i] = events[i].ID
	}

	if err := s.Sink(r.Context(), events); err != nil {
		metrics.EventsRejected.WithLabelValues("sink_error").Add(float64(len(events)))
		s.Logger.Error("sink failed", "err", err, "events", len(events))
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "temporarily unavailable, retry with the same ids"})
		return
	}
	metrics.EventsAccepted.WithLabelValues(s.Mode).Add(float64(len(events)))
	writeJSON(w, http.StatusAccepted, acceptedResponse{Accepted: len(events), IDs: ids})
}

// decodeEvents accepts either a single JSON object or a JSON array of objects.
func decodeEvents(r *http.Request) ([]event.Event, error) {
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '[' {
		var events []event.Event
		if err := json.Unmarshal(raw, &events); err != nil {
			return nil, fmt.Errorf("invalid event array: %w", err)
		}
		return events, nil
	}
	var e event.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, fmt.Errorf("invalid event: %w", err)
	}
	return []event.Event{e}, nil
}

// handleGet lets clients (and the demo) confirm an accepted event reached Postgres.
// 404 means "not stored yet": it may still be waiting in the stream.
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "id must be a UUID"})
		return
	}
	if s.Lookup == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "lookup not available"})
		return
	}
	e, err := s.Lookup(r.Context(), id)
	if err != nil {
		s.Logger.Error("lookup failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "temporarily unavailable"})
		return
	}
	if e == nil {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "not stored (yet)"})
		return
	}
	writeJSON(w, http.StatusOK, e)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.Draining != nil && s.Draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "shutting down"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Ready(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: err.Error()})
		return
	}
	w.WriteHeader(http.StatusOK)
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		metrics.RequestDuration.WithLabelValues(route, strconv.Itoa(rec.code)).Observe(time.Since(start).Seconds())
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
