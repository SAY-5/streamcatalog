# streamcatalog

A self-serve catalog for Kafka event streams. Teams register a stream with its
schema, owner, retention, and access model. Consumers discover streams and
subscribe to them through a REST API without a manual handoff. The catalog also
tracks lineage so you can see the producers and consumers upstream and
downstream of any stream.

This is a metadata and governance service. It catalogs and governs streams; it
does not move or process the data itself.

## What it does

- Register an event stream with a schema (JSON or Avro definition), an owner, a
  retention policy, tags, and an access model. Registration validates the input
  and ensures the backing Kafka topic exists.
- Discover streams by domain, owner, or tag.
- Read a stream's schema and its lineage (upstream producers and downstream
  consumers, traversed transitively).
- Request a subscription. The access model is checked server side; when it
  allows the consumer, the catalog records the subscription and a lineage edge
  so access is provisioned without manual steps.
- Evolve a schema. A new schema version is accepted only when it is a compatible
  evolution of the current one.

## Access model

Each stream declares one of three access models, enforced server side:

- `public`: any team may subscribe.
- `domain`: a team may subscribe only if it is in the same domain as the stream.
- `private`: a team may subscribe only if it is on the stream's allow list.

A read check (`GET /streams/{id}/access`) returns whether a given consumer is
allowed, either by the access model or by an existing subscription.

## Schema compatibility

When a stream's schema is updated, the new version must be both backward and
forward compatible with the previous version. For the field-based schemas used
here that means:

- No existing field may be removed.
- No existing field may change type.
- A newly added field must be optional.

A change that violates any of these rules is rejected and the version is not
bumped.

## Lineage

Lineage edges connect producers, streams, and consumers. Registering a stream
records a producer edge; subscribing records a consumer edge; a derivation edge
links one stream to another for a job that reads one and writes another. Lineage
queries traverse these edges transitively in both directions and terminate
safely even when the graph contains a cycle.

## REST API

| Method | Path | Description |
| ------ | ---- | ----------- |
| `POST` | `/streams` | Register a stream (producer team in `X-Team` header). |
| `GET` | `/streams` | Search by `domain`, `owner`, or `tag` query parameters. |
| `GET` | `/streams/{id}` | Get a stream including its schema. |
| `GET` | `/streams/{id}/lineage` | Get upstream and downstream nodes. |
| `PUT` | `/streams/{id}/schema` | Submit a new schema version. |
| `POST` | `/streams/{id}/subscriptions` | Self-serve subscribe. |
| `GET` | `/streams/{id}/access` | Check read access for a consumer. |

## Running locally

```
docker compose up --build
```

This starts Postgres, Kafka, and the catalog service on port 8080. Register a
stream:

```
curl -s -X POST localhost:8080/streams -H 'X-Team: team-orders' -d '{
  "name": "orders.v1",
  "topic": "orders-v1",
  "domain": "commerce",
  "owner": "team-orders",
  "tags": ["orders"],
  "retention_ms": 604800000,
  "access": "public",
  "schema_type": "json",
  "schema_def": "{\"fields\":{\"id\":{\"type\":\"string\"}}}"
}'
```

## Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `DATABASE_URL` | `postgres://catalog:catalog@localhost:5432/catalog?sslmode=disable` | Postgres connection string. |
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap broker. |
| `LISTEN_ADDR` | `:8080` | HTTP listen address. |

## Development

```
go test ./internal/... ./cmd/...
go test -tags integration ./internal/integration/...
golangci-lint run ./...
```

The integration suite starts real Postgres and Kafka containers with
Testcontainers, so it requires a working Docker environment. On a local colima
setup you may need to disable Ryuk (`TESTCONTAINERS_RYUK_DISABLED=true`) and
point `DOCKER_HOST` at the colima socket; CI is unaffected.

## How this differs

streamcatalog is a stream metadata catalog and governance layer: schema, owner,
retention, access model, lineage, and self-serve discovery and subscribe. It is
distinct from related projects:

- `kafka-pipeline` is an actual stream processing pipeline that moves and
  transforms records. streamcatalog does not process data; it catalogs and
  governs the streams a pipeline would read and write.
- `configmesh` distributes configuration. streamcatalog distributes stream
  metadata and access, not config.
- `ingestforge` handles ingestion. streamcatalog records lineage and governs
  access for streams rather than ingesting their contents.

The angle here is the catalog and metadata model plus the lineage graph, the
access model, and the self-serve subscribe flow.

## License

MIT, see [LICENSE](LICENSE).
