// Package catalog defines the domain model and storage for registered event streams.
package catalog

import (
	"errors"
	"time"
)

// AccessModel controls who may subscribe to a stream.
type AccessModel string

const (
	// AccessPublic allows any team to subscribe without approval.
	AccessPublic AccessModel = "public"
	// AccessDomain allows teams in the same domain to subscribe.
	AccessDomain AccessModel = "domain"
	// AccessPrivate allows only teams on the allow list to subscribe.
	AccessPrivate AccessModel = "private"
)

// Valid reports whether the access model is one of the known values.
func (a AccessModel) Valid() bool {
	switch a {
	case AccessPublic, AccessDomain, AccessPrivate:
		return true
	default:
		return false
	}
}

// SchemaType identifies the encoding of a stream schema.
type SchemaType string

const (
	// SchemaJSON is a JSON schema definition.
	SchemaJSON SchemaType = "json"
	// SchemaAvro is an Avro schema definition.
	SchemaAvro SchemaType = "avro"
)

// Valid reports whether the schema type is supported.
func (s SchemaType) Valid() bool {
	return s == SchemaJSON || s == SchemaAvro
}

// Stream is a registered event stream backed by a Kafka topic.
type Stream struct {
	ID            string      `json:"id"`
	Name          string      `json:"name"`
	Topic         string      `json:"topic"`
	Domain        string      `json:"domain"`
	Owner         string      `json:"owner"`
	Tags          []string    `json:"tags"`
	RetentionMS   int64       `json:"retention_ms"`
	Access        AccessModel `json:"access"`
	AllowList     []string    `json:"allow_list,omitempty"`
	SchemaType    SchemaType  `json:"schema_type"`
	SchemaVersion int         `json:"schema_version"`
	SchemaDef     string      `json:"schema_def"`
	CreatedAt     time.Time   `json:"created_at"`
}

// Subscription records a consumer's granted access to a stream.
type Subscription struct {
	ID        string    `json:"id"`
	StreamID  string    `json:"stream_id"`
	Consumer  string    `json:"consumer"`
	CreatedAt time.Time `json:"created_at"`
}

// LineageEdge records a directed producer-to-stream or stream-to-consumer relation.
type LineageEdge struct {
	ID       string `json:"id"`
	FromNode string `json:"from_node"`
	ToNode   string `json:"to_node"`
	StreamID string `json:"stream_id"`
	Kind     string `json:"kind"`
}

// Edge kinds.
const (
	EdgeProduce = "produce"
	EdgeConsume = "consume"
	EdgeDerive  = "derive"
)

// RegisterInput is the validated payload for registering a stream.
type RegisterInput struct {
	Name        string      `json:"name"`
	Topic       string      `json:"topic"`
	Domain      string      `json:"domain"`
	Owner       string      `json:"owner"`
	Tags        []string    `json:"tags"`
	RetentionMS int64       `json:"retention_ms"`
	Access      AccessModel `json:"access"`
	AllowList   []string    `json:"allow_list"`
	SchemaType  SchemaType  `json:"schema_type"`
	SchemaDef   string      `json:"schema_def"`
}

// Validation errors returned by the catalog.
var (
	ErrInvalidInput   = errors.New("invalid registration input")
	ErrStreamNotFound = errors.New("stream not found")
	ErrAccessDenied   = errors.New("access denied by access model")
	ErrTopicMissing   = errors.New("kafka topic does not exist")
	ErrSchemaConflict = errors.New("schema change is not compatible")
	ErrDuplicateName  = errors.New("stream name already registered")
)

// Validate checks a registration payload before it touches storage.
func (in RegisterInput) Validate() error {
	if in.Name == "" || in.Topic == "" || in.Owner == "" || in.Domain == "" {
		return ErrInvalidInput
	}
	if in.RetentionMS < 0 {
		return ErrInvalidInput
	}
	if !in.Access.Valid() {
		return ErrInvalidInput
	}
	if !in.SchemaType.Valid() {
		return ErrInvalidInput
	}
	if in.SchemaDef == "" {
		return ErrInvalidInput
	}
	if in.Access == AccessPrivate && len(in.AllowList) == 0 {
		return ErrInvalidInput
	}
	return nil
}
