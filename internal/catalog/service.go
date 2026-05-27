package catalog

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/SAY-5/streamcatalog/internal/lineage"
	"github.com/SAY-5/streamcatalog/internal/schema"
)

// TopicManager is the subset of Kafka admin behaviour the service depends on.
type TopicManager interface {
	CreateTopic(ctx context.Context, topic string, partitions int) error
	TopicExists(ctx context.Context, topic string) (bool, error)
}

// Repository is the persistence behaviour the service depends on. It is
// satisfied by the Postgres Store and by in-memory fakes in tests.
type Repository interface {
	InsertStream(ctx context.Context, st Stream) error
	UpdateSchema(ctx context.Context, id string, version int, def string) error
	GetStream(ctx context.Context, id string) (Stream, error)
	SearchStreams(ctx context.Context, f SearchFilter) ([]Stream, error)
	InsertSubscription(ctx context.Context, sub Subscription) error
	HasSubscription(ctx context.Context, streamID, consumer string) (bool, error)
	InsertEdge(ctx context.Context, e LineageEdge) error
	AllEdges(ctx context.Context) ([]LineageEdge, error)
}

// Service implements the catalog use cases on top of a Repository and Kafka admin.
type Service struct {
	store Repository
	kafka TopicManager
}

// NewService wires a repository and Kafka topic manager into a service.
func NewService(store Repository, km TopicManager) *Service {
	return &Service{store: store, kafka: km}
}

// Register validates input, ensures the Kafka topic exists, stores the stream,
// and records the producer-to-stream lineage edge.
func (s *Service) Register(ctx context.Context, in RegisterInput, producer string) (Stream, error) {
	if err := in.Validate(); err != nil {
		return Stream{}, err
	}
	if _, err := schema.Parse(in.SchemaDef); err != nil {
		return Stream{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if err := s.kafka.CreateTopic(ctx, in.Topic, 1); err != nil {
		return Stream{}, err
	}
	exists, err := s.kafka.TopicExists(ctx, in.Topic)
	if err != nil {
		return Stream{}, err
	}
	if !exists {
		return Stream{}, ErrTopicMissing
	}
	st := Stream{
		ID:            uuid.NewString(),
		Name:          in.Name,
		Topic:         in.Topic,
		Domain:        in.Domain,
		Owner:         in.Owner,
		Tags:          in.Tags,
		RetentionMS:   in.RetentionMS,
		Access:        in.Access,
		AllowList:     in.AllowList,
		SchemaType:    in.SchemaType,
		SchemaVersion: 1,
		SchemaDef:     in.SchemaDef,
	}
	if st.Tags == nil {
		st.Tags = []string{}
	}
	if st.AllowList == nil {
		st.AllowList = []string{}
	}
	if insErr := s.store.InsertStream(ctx, st); insErr != nil {
		return Stream{}, insErr
	}
	if producer != "" {
		edge := LineageEdge{
			ID:       uuid.NewString(),
			FromNode: "team:" + producer,
			ToNode:   "stream:" + st.ID,
			StreamID: st.ID,
			Kind:     EdgeProduce,
		}
		if edgeErr := s.store.InsertEdge(ctx, edge); edgeErr != nil {
			return Stream{}, edgeErr
		}
	}
	return s.store.GetStream(ctx, st.ID)
}

// Get returns a stream by ID.
func (s *Service) Get(ctx context.Context, id string) (Stream, error) {
	return s.store.GetStream(ctx, id)
}

// Search discovers streams by domain, owner, or tag.
func (s *Service) Search(ctx context.Context, f SearchFilter) ([]Stream, error) {
	return s.store.SearchStreams(ctx, f)
}

// allows reports whether the access model permits a consumer to subscribe.
func allows(st Stream, consumerDomain, consumer string) bool {
	switch st.Access {
	case AccessPublic:
		return true
	case AccessDomain:
		return consumerDomain != "" && consumerDomain == st.Domain
	case AccessPrivate:
		for _, allowed := range st.AllowList {
			if allowed == consumer {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// SubscribeResult reports the outcome of a self-serve subscription.
type SubscribeResult struct {
	Subscription Subscription
	Edge         LineageEdge
}

// Subscribe enforces the access model server-side and, when allowed, provisions
// access by recording the subscription and a stream-to-consumer lineage edge.
func (s *Service) Subscribe(ctx context.Context, streamID, consumer, consumerDomain string) (SubscribeResult, error) {
	if consumer == "" {
		return SubscribeResult{}, ErrInvalidInput
	}
	st, err := s.store.GetStream(ctx, streamID)
	if err != nil {
		return SubscribeResult{}, err
	}
	if !allows(st, consumerDomain, consumer) {
		return SubscribeResult{}, ErrAccessDenied
	}
	sub := Subscription{ID: uuid.NewString(), StreamID: st.ID, Consumer: consumer}
	if subErr := s.store.InsertSubscription(ctx, sub); subErr != nil {
		return SubscribeResult{}, subErr
	}
	edge := LineageEdge{
		ID:       uuid.NewString(),
		FromNode: "stream:" + st.ID,
		ToNode:   "team:" + consumer,
		StreamID: st.ID,
		Kind:     EdgeConsume,
	}
	if edgeErr := s.store.InsertEdge(ctx, edge); edgeErr != nil {
		return SubscribeResult{}, edgeErr
	}
	return SubscribeResult{Subscription: sub, Edge: edge}, nil
}

// CanRead enforces the read authorization boundary: a consumer may read a
// stream only if the access model allows it or it already holds a subscription.
func (s *Service) CanRead(ctx context.Context, streamID, consumer, consumerDomain string) (bool, error) {
	st, err := s.store.GetStream(ctx, streamID)
	if err != nil {
		return false, err
	}
	if allows(st, consumerDomain, consumer) {
		return true, nil
	}
	return s.store.HasSubscription(ctx, streamID, consumer)
}

// UpdateSchema accepts a new schema version only when it is a compatible
// evolution of the current one, then bumps the version.
func (s *Service) UpdateSchema(ctx context.Context, streamID, newDef string) (Stream, error) {
	st, err := s.store.GetStream(ctx, streamID)
	if err != nil {
		return Stream{}, err
	}
	ok, issues, compatErr := schema.Compatible(st.SchemaDef, newDef)
	if compatErr != nil {
		return Stream{}, fmt.Errorf("%w: %v", ErrInvalidInput, compatErr)
	}
	if !ok {
		return Stream{}, fmt.Errorf("%w: %v", ErrSchemaConflict, issues)
	}
	if updErr := s.store.UpdateSchema(ctx, streamID, st.SchemaVersion+1, newDef); updErr != nil {
		return Stream{}, updErr
	}
	return s.store.GetStream(ctx, streamID)
}

func toGraphEdges(edges []LineageEdge) []lineage.Edge {
	out := make([]lineage.Edge, len(edges))
	for i, e := range edges {
		out[i] = lineage.Edge{From: e.FromNode, To: e.ToNode}
	}
	return out
}

// LineageView is the upstream/downstream answer for a stream.
type LineageView struct {
	Node       string   `json:"node"`
	Upstream   []string `json:"upstream"`
	Downstream []string `json:"downstream"`
}

// Lineage builds the lineage graph and returns the transitive upstream and
// downstream nodes of a stream.
func (s *Service) Lineage(ctx context.Context, streamID string) (LineageView, error) {
	if _, err := s.store.GetStream(ctx, streamID); err != nil {
		return LineageView{}, err
	}
	edges, err := s.store.AllEdges(ctx)
	if err != nil {
		return LineageView{}, err
	}
	g := lineage.Build(toGraphEdges(edges))
	node := "stream:" + streamID
	return LineageView{
		Node:       node,
		Upstream:   g.Upstream(node),
		Downstream: g.Downstream(node),
	}, nil
}

// AddDerivation records a derive edge from one stream to another, modelling a
// processing job that reads one stream and writes another.
func (s *Service) AddDerivation(ctx context.Context, fromStreamID, toStreamID string) error {
	if _, err := s.store.GetStream(ctx, fromStreamID); err != nil {
		return err
	}
	if _, err := s.store.GetStream(ctx, toStreamID); err != nil {
		return err
	}
	edge := LineageEdge{
		ID:       uuid.NewString(),
		FromNode: "stream:" + fromStreamID,
		ToNode:   "stream:" + toStreamID,
		StreamID: toStreamID,
		Kind:     EdgeDerive,
	}
	return s.store.InsertEdge(ctx, edge)
}
