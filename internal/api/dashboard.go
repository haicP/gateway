package api

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"net/http"
)

//go:embed dashboard.html
var dashboardHTML []byte

// DashboardHandler serves the lightweight trace dashboard.
func DashboardHandler(providerPrefixes []string) http.Handler {
	prefixes := normalizeDashboardProviderPrefixes(providerPrefixes)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(renderDashboardHTML(prefixes))
	})
}

func renderDashboardHTML(providerPrefixes []string) []byte {
	encoded, err := json.Marshal(normalizeDashboardProviderPrefixes(providerPrefixes))
	if err != nil {
		encoded = []byte("[]")
	}
	return bytes.ReplaceAll(dashboardHTML, []byte("__PROVIDER_PREFIXES__"), encoded)
}
