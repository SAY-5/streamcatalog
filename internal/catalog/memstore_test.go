package catalog

import (
	"context"
	"sync"
)

// memStore is an in-memory Repository used by service unit tests. It mirrors
// the constraints the Postgres store enforces: unique names and rejecting edges
// that reference an unknown stream.
type memStore struct {
	mu      sync.Mutex
	streams map[string]Stream
	names   map[string]struct{}
	subs    map[string]Subscription
	edges   []LineageEdge
}

func newMemStore() *memStore {
	return &memStore{
		streams: map[string]Stream{},
		names:   map[string]struct{}{},
		subs:    map[string]Subscription{},
	}
}

func (m *memStore) InsertStream(_ context.Context, st Stream) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.names[st.Name]; ok {
		return ErrDuplicateName
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
		return ErrStreamNotFound
	}
	st.SchemaVersion = version
	st.SchemaDef = def
	m.streams[id] = st
	return nil
}

func (m *memStore) GetStream(_ context.Context, id string) (Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.streams[id]
	if !ok {
		return Stream{}, ErrStreamNotFound
	}
	return st, nil
}

func (m *memStore) SearchStreams(_ context.Context, f SearchFilter) ([]Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Stream, 0)
	for _, st := range m.streams {
		if f.Domain != "" && st.Domain != f.Domain {
			continue
		}
		if f.Owner != "" && st.Owner != f.Owner {
			continue
		}
		if f.Tag != "" && !containsTag(st.Tags, f.Tag) {
			continue
		}
		out = append(out, st)
	}
	return out, nil
}

func containsTag(tags []string, tag string) bool {
	for _, t := range tags {
		if t == tag {
			return true
		}
	}
	return false
}

func (m *memStore) InsertSubscription(_ context.Context, sub Subscription) error {
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

func (m *memStore) InsertEdge(_ context.Context, e LineageEdge) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.streams[e.StreamID]; !ok {
		return ErrStreamNotFound
	}
	m.edges = append(m.edges, e)
	return nil
}

func (m *memStore) AllEdges(_ context.Context) ([]LineageEdge, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]LineageEdge, len(m.edges))
	copy(out, m.edges)
	return out, nil
}

// fakeKafka is a TopicManager that records created topics in memory.
type fakeKafka struct {
	topics map[string]struct{}
}

func newFakeKafka() *fakeKafka { return &fakeKafka{topics: map[string]struct{}{}} }

func (f *fakeKafka) CreateTopic(_ context.Context, topic string, _ int) error {
	f.topics[topic] = struct{}{}
	return nil
}

func (f *fakeKafka) TopicExists(_ context.Context, topic string) (bool, error) {
	_, ok := f.topics[topic]
	return ok, nil
}
