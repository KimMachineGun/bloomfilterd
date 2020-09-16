package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/KimMachineGun/bfd/internal/store"
)

type Handler struct {
	store *store.Store
}

func NewHandler(s *store.Store) *Handler {
	return &Handler{
		store: s,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case path == "/join":
		h.handleJoin(w, r)
	case path == "/snapshot":
		h.handleSnapshot(w, r)
	case strings.HasPrefix(path, "/key/"):
		h.handleOp(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}

	var msg JoinMessage
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.store.Join(msg.NodeID, msg.Addr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) handleOp(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/key/"):]

	switch r.Method {
	case http.MethodGet:
		res, err := h.store.Check(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, res)
	case http.MethodPost:
		res, err := h.store.Set(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, res)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}

	err := h.store.Snapshot()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
