// Package store writes events to Postgres.
package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/msalimcengiz/go-event-ingestor/internal/event"
	"github.com/msalimcengiz/go-event-ingestor/migrations"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(ctx context.Context, url string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() { p.pool.Close() }

func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

// Migrate applies every embedded .sql file in name order. Files are idempotent.
func (p *Postgres) Migrate(ctx context.Context) error {
	names, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		sql, err := migrations.FS.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := p.pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

// InsertOne is the per-request write used by WRITE_MODE=direct.
func (p *Postgres) InsertOne(ctx context.Context, e event.Event) error {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO events (id, type, user_id, occurred_at, payload)
		 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING`,
		e.ID, e.Type, e.UserID, e.OccurredAt, []byte(e.Payload))
	return err
}

// InsertBatch writes a batch in a single transaction: COPY into a temporary staging
// table, then INSERT ... ON CONFLICT DO NOTHING so redelivered messages stay idempotent.
// It returns the number of rows that were new.
func (p *Postgres) InsertBatch(ctx context.Context, events []event.Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	if _, err := tx.Exec(ctx,
		`CREATE TEMP TABLE events_stage (LIKE events INCLUDING DEFAULTS) ON COMMIT DROP`); err != nil {
		return 0, err
	}

	rows := make([][]any, len(events))
	for i, e := range events {
		rows[i] = []any{e.ID, e.Type, e.UserID, e.OccurredAt, []byte(e.Payload)}
	}
	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{"events_stage"},
		[]string{"id", "type", "user_id", "occurred_at", "payload"},
		pgx.CopyFromRows(rows)); err != nil {
		return 0, fmt.Errorf("copy: %w", err)
	}

	tag, err := tx.Exec(ctx,
		`INSERT INTO events (id, type, user_id, occurred_at, received_at, payload)
		 SELECT id, type, user_id, occurred_at, received_at, payload FROM events_stage
		 ON CONFLICT (id) DO NOTHING`)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Find returns the stored event with this id, or nil.
func (p *Postgres) Find(ctx context.Context, id uuid.UUID) (*event.Event, error) {
	var e event.Event
	var payload []byte
	err := p.pool.QueryRow(ctx,
		`SELECT id, type, user_id, occurred_at, payload FROM events WHERE id = $1`, id,
	).Scan(&e.ID, &e.Type, &e.UserID, &e.OccurredAt, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	e.Payload = payload
	e.OccurredAt = e.OccurredAt.UTC()
	return &e, nil
}

// CountIDs reports how many of the given IDs are stored. Used by tests and the benchmark.
func (p *Postgres) CountIDs(ctx context.Context, ids []uuid.UUID) (int, error) {
	var n int
	err := p.pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE id = ANY($1)`, ids).Scan(&n)
	return n, err
}
