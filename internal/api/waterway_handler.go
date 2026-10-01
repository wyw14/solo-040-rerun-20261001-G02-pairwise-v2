package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

func waterwayHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, runtime.Waterway.Status())
	}
}

type waterwayReleaseInput struct {
	Reason string `json:"reason"`
}

func releaseOccupationHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		occupationID, err := parseUUID(chi.URLParam(r, "occupationID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		var input waterwayReleaseInput
		if r.ContentLength > 0 {
			if err := decodeJSON(r, &input); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
		}
		if input.Reason == "" {
			input.Reason = "operator release"
		}
		occupation, err := runtime.Waterway.ReleaseOccupation(r.Context(), occupationID, input.Reason)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, occupation)
	}
}
