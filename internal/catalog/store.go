package catalog

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Store persists streams, subscriptions, and lineage edges in Postgres.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore wraps an existing pgx pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Migrate applies the embedded schema migrations.
func (s *Store) Migrate(ctx context.Context) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		body, readErr := migrationFS.ReadFile("migrations/" + name)
		if readErr != nil {
			return fmt.Errorf("read %s: %w", name, readErr)
		}
		if _, execErr := s.pool.Exec(ctx, string(body)); execErr != nil {
			return fmt.Errorf("apply %s: %w", name, execErr)
		}
	}
	return nil
}

// InsertStream stores a fully built stream record.
func (s *Store) InsertStream(ctx context.Context, st Stream) error {
	const q = `INSERT INTO streams
		(id, name, topic, domain, owner, tags, retention_ms, access, allow_list, schema_type, schema_version, schema_def)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`
	_, err := s.pool.Exec(ctx, q,
		st.ID, st.Name, st.Topic, st.Domain, st.Owner, st.Tags, st.RetentionMS,
		string(st.Access), st.AllowList, string(st.SchemaType), st.SchemaVersion, st.SchemaDef)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrDuplicateName
		}
		return fmt.Errorf("insert stream: %w", err)
	}
	return nil
}

// UpdateSchema bumps a stream's schema version and definition.
func (s *Store) UpdateSchema(ctx context.Context, id string, version int, def string) error {
	const q = `UPDATE streams SET schema_version = $2, schema_def = $3 WHERE id = $1`
	tag, err := s.pool.Exec(ctx, q, id, version, def)
	if err != nil {
		return fmt.Errorf("update schema: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrStreamNotFound
	}
	return nil
}

func scanStream(row pgx.Row) (Stream, error) {
	var st Stream
	var access, schemaType string
	err := row.Scan(&st.ID, &st.Name, &st.Topic, &st.Domain, &st.Owner, &st.Tags,
		&st.RetentionMS, &access, &st.AllowList, &schemaType, &st.SchemaVersion, &st.SchemaDef, &st.CreatedAt)
	if err != nil {
		return Stream{}, err
	}
	st.Access = AccessModel(access)
	st.SchemaType = SchemaType(schemaType)
	return st, nil
}

const streamColumns = `id, name, topic, domain, owner, tags, retention_ms, access, allow_list, schema_type, schema_version, schema_def, created_at`

// GetStream fetches a stream by ID.
func (s *Store) GetStream(ctx context.Context, id string) (Stream, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+streamColumns+` FROM streams WHERE id = $1`, id)
	st, err := scanStream(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stream{}, ErrStreamNotFound
	}
	if err != nil {
		return Stream{}, fmt.Errorf("get stream: %w", err)
	}
	return st, nil
}

// SearchFilter narrows a stream search.
type SearchFilter struct {
	Domain string
	Owner  string
	Tag    string
}

// SearchStreams returns streams matching all provided filters.
func (s *Store) SearchStreams(ctx context.Context, f SearchFilter) ([]Stream, error) {
	conds := make([]string, 0, 3)
	args := make([]any, 0, 3)
	idx := 1
	if f.Domain != "" {
		conds = append(conds, fmt.Sprintf("domain = $%d", idx))
		args = append(args, f.Domain)
		idx++
	}
	if f.Owner != "" {
		conds = append(conds, fmt.Sprintf("owner = $%d", idx))
		args = append(args, f.Owner)
		idx++
	}
	if f.Tag != "" {
		conds = append(conds, fmt.Sprintf("$%d = ANY(tags)", idx))
		args = append(args, f.Tag)
	}
	q := `SELECT ` + streamColumns + ` FROM streams`
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	q += " ORDER BY name"
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("search streams: %w", err)
	}
	defer rows.Close()
	out := make([]Stream, 0)
	for rows.Next() {
		st, scanErr := scanStream(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan stream: %w", scanErr)
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// InsertSubscription records a granted subscription.
func (s *Store) InsertSubscription(ctx context.Context, sub Subscription) error {
	const q = `INSERT INTO subscriptions (id, stream_id, consumer) VALUES ($1,$2,$3)
		ON CONFLICT (stream_id, consumer) DO NOTHING`
	_, err := s.pool.Exec(ctx, q, sub.ID, sub.StreamID, sub.Consumer)
	if err != nil {
		return fmt.Errorf("insert subscription: %w", err)
	}
	return nil
}

// HasSubscription reports whether a consumer already subscribes to a stream.
func (s *Store) HasSubscription(ctx context.Context, streamID, consumer string) (bool, error) {
	const q = `SELECT 1 FROM subscriptions WHERE stream_id = $1 AND consumer = $2`
	var x int
	err := s.pool.QueryRow(ctx, q, streamID, consumer).Scan(&x)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("has subscription: %w", err)
	}
	return true, nil
}

// InsertEdge records a lineage edge. It rejects edges that reference a stream
// not present in the streams table via the foreign key constraint.
func (s *Store) InsertEdge(ctx context.Context, e LineageEdge) error {
	const q = `INSERT INTO lineage_edges (id, from_node, to_node, stream_id, kind)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT (from_node, to_node, stream_id, kind) DO NOTHING`
	_, err := s.pool.Exec(ctx, q, e.ID, e.FromNode, e.ToNode, e.StreamID, e.Kind)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return ErrStreamNotFound
		}
		return fmt.Errorf("insert edge: %w", err)
	}
	return nil
}

// AllEdges returns every lineage edge.
func (s *Store) AllEdges(ctx context.Context) ([]LineageEdge, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, from_node, to_node, stream_id, kind FROM lineage_edges`)
	if err != nil {
		return nil, fmt.Errorf("all edges: %w", err)
	}
	defer rows.Close()
	out := make([]LineageEdge, 0)
	for rows.Next() {
		var e LineageEdge
		if scanErr := rows.Scan(&e.ID, &e.FromNode, &e.ToNode, &e.StreamID, &e.Kind); scanErr != nil {
			return nil, fmt.Errorf("scan edge: %w", scanErr)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
