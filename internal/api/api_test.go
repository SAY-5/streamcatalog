package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/SAY-5/streamcatalog/internal/catalog"
)

// memStore mirrors the Postgres store's observable behaviour for HTTP contract
// tests: unique names, foreign-key style edge rejection, and search filters.
type memStore struct {
	mu      sync.Mutex
	streams map[string]catalog.Stream
	names   map[string]struct{}
	subs    map[string]catalog.Subscription
	edges   []catalog.LineageEdge
}

func newMemStore() *memStore {
	return &memStore{
		streams: map[string]catalog.Stream{},
		names:   map[string]struct{}{},
		subs:    map[string]catalog.Subscription{},
	}
}

func (m *memStore) InsertStream(_ context.Context, st catalog.Stream) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.names[st.Name]; ok {
		return catalog.ErrDuplicateName
	}
	m.names[st.Name] = struct{}{}
	m.streams[st.ID] = st
	return nil
}

func (m *memStore) UpdateSchema(_ context.Context, id string, version int, def string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.streams[id]
	if !ok {
		return catalog.ErrStreamNotFound
	}
	st.SchemaVersion = version
	st.SchemaDef = def
	m.streams[id] = st
	return nil
}

func (m *memStore) GetStream(_ context.Context, id string) (catalog.Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.streams[id]
	if !ok {
		return catalog.Stream{}, catalog.ErrStreamNotFound
	}
	return st, nil
}

func (m *memStore) SearchStreams(_ context.Context, f catalog.SearchFilter) ([]catalog.Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]catalog.Stream, 0)
	for _, st := range m.streams {
		if f.Domain != "" && st.Domain != f.Domain {
			continue
		}
		if f.Owner != "" && st.Owner != f.Owner {
			continue
		}
		if f.Tag != "" {
			match := false
			for _, t := range st.Tags {
				if t == f.Tag {
					match = true
				}
			}
			if !match {
				continue
			}
		}
		out = append(out, st)
	}
	return out, nil
}

func (m *memStore) InsertSubscription(_ context.Context, sub catalog.Subscription) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subs[sub.StreamID+"|"+sub.Consumer] = sub
	return nil
}

func (m *memStore) HasSubscription(_ context.Context, streamID, consumer string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.subs[streamID+"|"+consumer]
	return ok, nil
}

func (m *memStore) InsertEdge(_ context.Context, e catalog.LineageEdge) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.streams[e.StreamID]; !ok {
		return catalog.ErrStreamNotFound
	}
	m.edges = append(m.edges, e)
	return nil
}

func (m *memStore) AllEdges(_ context.Context) ([]catalog.LineageEdge, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]catalog.LineageEdge, len(m.edges))
	copy(out, m.edges)
	return out, nil
}

type fakeKafka struct{ topics map[string]struct{} }

func (f *fakeKafka) CreateTopic(_ context.Context, topic string, _ int) error {
	f.topics[topic] = struct{}{}
	return nil
}

func (f *fakeKafka) TopicExists(_ context.Context, topic string) (bool, error) {
	_, ok := f.topics[topic]
	return ok, nil
}

func newTestServer() (*memStore, http.Handler) {
	store := newMemStore()
	svc := catalog.NewService(store, &fakeKafka{topics: map[string]struct{}{}})
	return store, NewServer(svc).Routes()
}

func registerBody(name string) string {
	return `{"name":"` + name + `","topic":"t-` + name + `","domain":"commerce",` +
		`"owner":"team-orders","tags":["orders"],"retention_ms":604800000,` +
		`"access":"public","schema_type":"json","schema_def":"{\"fields\":{\"id\":{\"type\":\"string\"}}}"}`
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, bytes.NewBufferString(body))
	}
	r.Header.Set("X-Team", "team-orders")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// A registered stream must be discoverable through search.
func TestRegisteredStreamIsDiscoverable(t *testing.T) {
	_, h := newTestServer()

	w := do(t, h, http.MethodPost, "/streams", registerBody("orders.v1"))
	if w.Code != http.StatusCreated {
		t.Fatalf("register status = %d, body = %s", w.Code, w.Body.String())
	}
	var created catalog.Stream
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}

	search := do(t, h, http.MethodGet, "/streams?domain=commerce", "")
	if !strings.Contains(search.Body.String(), created.ID) {
		t.Fatalf("registered stream %s not found in search: %s", created.ID, search.Body.String())
	}

	byTag := do(t, h, http.MethodGet, "/streams?tag=orders", "")
	if !strings.Contains(byTag.Body.String(), created.ID) {
		t.Fatalf("registered stream not found by tag: %s", byTag.Body.String())
	}
}

// The REST contract: known error conditions map to documented status codes.
func TestRestContractStatusCodes(t *testing.T) {
	_, h := newTestServer()

	if w := do(t, h, http.MethodGet, "/streams/missing", ""); w.Code != http.StatusNotFound {
		t.Fatalf("missing stream status = %d, want 404", w.Code)
	}
	if w := do(t, h, http.MethodPost, "/streams", `{"name":"x"}`); w.Code != http.StatusBadRequest {
		t.Fatalf("invalid register status = %d, want 400", w.Code)
	}

	do(t, h, http.MethodPost, "/streams", registerBody("dup.v1"))
	if w := do(t, h, http.MethodPost, "/streams", registerBody("dup.v1")); w.Code != http.StatusConflict {
		t.Fatalf("duplicate name status = %d, want 409", w.Code)
	}
}

// Access checks must enforce the access model server side.
func TestAccessCheckEnforcesModel(t *testing.T) {
	store, h := newTestServer()

	// Register a private stream directly through the service so we control the
	// allow list precisely.
	svc := catalog.NewService(store, &fakeKafka{topics: map[string]struct{}{}})
	in := catalog.RegisterInput{
		Name: "secret.v1", Topic: "t-secret", Domain: "fin", Owner: "team-fin",
		Tags: []string{}, RetentionMS: 1000, Access: catalog.AccessPrivate,
		AllowList: []string{"team-allowed"}, SchemaType: catalog.SchemaJSON,
		SchemaDef: `{"fields":{"id":{"type":"string"}}}`,
	}
	st, err := svc.Register(context.Background(), in, "team-fin")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	denied := do(t, h, http.MethodPost, "/streams/"+st.ID+"/subscriptions",
		`{"consumer":"team-other","consumer_domain":"marketing"}`)
	if denied.Code != http.StatusForbidden {
		t.Fatalf("disallowed subscribe status = %d, want 403", denied.Code)
	}

	allowed := do(t, h, http.MethodPost, "/streams/"+st.ID+"/subscriptions",
		`{"consumer":"team-allowed","consumer_domain":"fin"}`)
	if allowed.Code != http.StatusCreated {
		t.Fatalf("allowed subscribe status = %d, want 201", allowed.Code)
	}

	check := do(t, h, http.MethodGet, "/streams/"+st.ID+"/access?consumer=team-allowed&consumer_domain=fin", "")
	if !strings.Contains(check.Body.String(), `"allowed":true`) {
		t.Fatalf("access check for allowed consumer = %s", check.Body.String())
	}
}

// Lineage edges must stay consistent: every edge a subscribe creates references
// the registered stream, so no lineage node points at a stream that is absent.
func TestLineageEdgesReferenceExistingStreams(t *testing.T) {
	store, h := newTestServer()

	w := do(t, h, http.MethodPost, "/streams", registerBody("events.v1"))
	var st catalog.Stream
	_ = json.Unmarshal(w.Body.Bytes(), &st)

	do(t, h, http.MethodPost, "/streams/"+st.ID+"/subscriptions",
		`{"consumer":"team-reader","consumer_domain":"any"}`)

	edges, err := store.AllEdges(context.Background())
	if err != nil {
		t.Fatalf("all edges: %v", err)
	}
	if len(edges) == 0 {
		t.Fatal("expected at least one lineage edge")
	}
	known := map[string]struct{}{st.ID: {}}
	for _, e := range edges {
		if _, ok := known[e.StreamID]; !ok {
			t.Fatalf("edge %s references missing stream %s", e.ID, e.StreamID)
		}
	}
}
