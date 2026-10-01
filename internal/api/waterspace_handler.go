package api

import "net/http"

// waterspaceHandler returns the shared pilotage-reach arbitration board:
// which convoys currently occupy each reach and the blocking reason for every
// convoy waiting to enter.
func waterspaceHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"waterways": runtime.Waterways.Views(),
		})
	}
}
