// Package api exposes the catalog over a REST HTTP interface.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/SAY-5/streamcatalog/internal/catalog"
)

// Server adapts a catalog service to HTTP handlers.
type Server struct {
	svc *catalog.Service
}

// NewServer builds an HTTP server over a catalog service.
func NewServer(svc *catalog.Service) *Server {
	return &Server{svc: svc}
}

// Routes returns the configured HTTP mux.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /streams", s.register)
	mux.HandleFunc("GET /streams", s.search)
	mux.HandleFunc("GET /streams/{id}", s.get)
	mux.HandleFunc("GET /streams/{id}/lineage", s.lineage)
	mux.HandleFunc("PUT /streams/{id}/schema", s.updateSchema)
	mux.HandleFunc("POST /streams/{id}/subscriptions", s.subscribe)
	mux.HandleFunc("GET /streams/{id}/access", s.access)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeErr(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, catalog.ErrInvalidInput):
		status = http.StatusBadRequest
	case errors.Is(err, catalog.ErrStreamNotFound):
		status = http.StatusNotFound
	case errors.Is(err, catalog.ErrAccessDenied):
		status = http.StatusForbidden
	case errors.Is(err, catalog.ErrDuplicateName):
		status = http.StatusConflict
	case errors.Is(err, catalog.ErrSchemaConflict):
		status = http.StatusConflict
	case errors.Is(err, catalog.ErrTopicMissing):
		status = http.StatusBadGateway
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var in catalog.RegisterInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeErr(w, catalog.ErrInvalidInput)
		return
	}
	producer := r.Header.Get("X-Team")
	st, err := s.svc.Register(r.Context(), in, producer)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, st)
}

func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	f := catalog.SearchFilter{
		Domain: r.URL.Query().Get("domain"),
		Owner:  r.URL.Query().Get("owner"),
		Tag:    r.URL.Query().Get("tag"),
	}
	streams, err := s.svc.Search(r.Context(), f)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"streams": streams})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	st, err := s.svc.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) lineage(w http.ResponseWriter, r *http.Request) {
	view, err := s.svc.Lineage(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

type schemaUpdate struct {
	SchemaDef string `json:"schema_def"`
}

func (s *Server) updateSchema(w http.ResponseWriter, r *http.Request) {
	var body schemaUpdate
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, catalog.ErrInvalidInput)
		return
	}
	st, err := s.svc.UpdateSchema(r.Context(), r.PathValue("id"), body.SchemaDef)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

type subscribeRequest struct {
	Consumer       string `json:"consumer"`
	ConsumerDomain string `json:"consumer_domain"`
}

func (s *Server) subscribe(w http.ResponseWriter, r *http.Request) {
	var body subscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, catalog.ErrInvalidInput)
		return
	}
	res, err := s.svc.Subscribe(r.Context(), r.PathValue("id"), body.Consumer, body.ConsumerDomain)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, res)
}

func (s *Server) access(w http.ResponseWriter, r *http.Request) {
	consumer := r.URL.Query().Get("consumer")
	domain := r.URL.Query().Get("consumer_domain")
	ok, err := s.svc.CanRead(r.Context(), r.PathValue("id"), consumer, domain)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"allowed": ok})
}
