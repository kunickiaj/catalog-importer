package source_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/incident-io/catalog-importer/v2/source"
	"github.com/stretchr/testify/assert"
)

func TestSourceBackstage_Load(t *testing.T) {
	t.Run("loads entries from Backstage API with query parameters", func(t *testing.T) {
		// Create a test server to mock the Backstage API
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "/api/catalog/entities/by-query", r.URL.Path)
			assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
			assert.Equal(t, "kind=Component", r.URL.Query().Get("filter"))
			assert.Equal(t, "10", r.URL.Query().Get("limit"))
			assert.Equal(t, "0", r.URL.Query().Get("offset"))

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[{"metadata": {"name": "component1"}}, {"metadata": {"name": "component2"}}]`))
		}))
		defer server.Close()

		sourceBackstage := source.SourceBackstage{
			Endpoint: server.URL + "/api/catalog/entities/by-query",
			Token:    "test-token",
			SignJWT:  nil,
			Filters:  "kind=Component",
			Limit:    10,
			Offset:   0,
		}

		entries, err := sourceBackstage.Load(context.Background(), nil)
		assert.NoError(t, err)
		assert.Len(t, entries, 2)
		assert.JSONEq(t, `{"metadata": {"name": "component1"}}`, string(entries[0].Content))
		assert.JSONEq(t, `{"metadata": {"name": "component2"}}`, string(entries[1].Content))
	})
}
