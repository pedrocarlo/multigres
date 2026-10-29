// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hrana

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValueJSONRoundTrip(t *testing.T) {
	values := []Value{
		Null(),
		{Type: ValueInteger, Int: -9007199254740993},
		{Type: ValueFloat, Float: 1.5},
		Text("a\tb"),
		{Type: ValueBlob, Blob: []byte{0, 1, 2, 255}},
	}
	b, err := json.Marshal(values)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"type":"null"},{"type":"integer","value":"-9007199254740993"},{"type":"float","value":1.5},{"type":"text","value":"a\tb"},{"type":"blob","base64":"AAEC/w=="}]`, string(b))

	var got []Value
	require.NoError(t, json.Unmarshal(b, &got))
	assert.Equal(t, values, got)
}

func TestValueUnmarshalUnpaddedBlob(t *testing.T) {
	var v Value
	require.NoError(t, json.Unmarshal([]byte(`{"type":"blob","base64":"AAEC/w"}`), &v))
	assert.Equal(t, []byte{0, 1, 2, 255}, v.Blob)
}

// fakeServer answers pipelines and records what it received.
type fakeServer struct {
	requests []PipelineRequest
	hosts    []string
	auth     []string
	respond  func(PipelineRequest) PipelineResponse
}

func (f *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v3/pipeline" {
		http.NotFound(w, r)
		return
	}
	var req PipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.requests = append(f.requests, req)
	f.hosts = append(f.hosts, r.Host)
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	_ = json.NewEncoder(w).Encode(f.respond(req))
}

func okExecute(t *testing.T, res StmtResult) StreamResult {
	raw, err := json.Marshal(res)
	require.NoError(t, err)
	return StreamResult{Type: "ok", Response: &StreamResponse{Type: "execute", Result: raw}}
}

func TestStreamKeepsBatonAndClosesOnClose(t *testing.T) {
	baton := "b1"
	f := &fakeServer{}
	f.respond = func(req PipelineRequest) PipelineResponse {
		results := make([]StreamResult, len(req.Requests))
		for i, r := range req.Requests {
			if r.Type == "close" {
				results[i] = StreamResult{Type: "ok", Response: &StreamResponse{Type: "close"}}
				continue
			}
			results[i] = okExecute(t, StmtResult{AffectedRowCount: 1})
		}
		if req.Requests[len(req.Requests)-1].Type == "close" {
			return PipelineResponse{Results: results}
		}
		return PipelineResponse{Baton: &baton, Results: results}
	}
	srv := httptest.NewServer(f)
	defer srv.Close()

	c := NewClient(Config{URL: srv.URL + "/", Host: "db--t--g.turso", Token: func() (string, error) { return "tok", nil }})
	s := c.NewStream()
	_, err := s.Pipeline(context.Background(), ExecuteRequest(Stmt{SQL: "BEGIN"}))
	require.NoError(t, err)
	_, err = s.Pipeline(context.Background(), ExecuteRequest(Stmt{SQL: "SELECT 1", WantRows: true}))
	require.NoError(t, err)
	require.NoError(t, s.Close(context.Background()))
	// A closed stream sends nothing more.
	require.NoError(t, s.Close(context.Background()))
	_, err = s.Pipeline(context.Background(), ExecuteRequest(Stmt{SQL: "SELECT 1"}))
	require.ErrorIs(t, err, ErrStreamClosed)

	require.Len(t, f.requests, 3)
	assert.Nil(t, f.requests[0].Baton)
	require.NotNil(t, f.requests[1].Baton)
	assert.Equal(t, "b1", *f.requests[1].Baton)
	assert.Equal(t, "close", f.requests[2].Requests[0].Type)
	assert.Equal(t, []string{"db--t--g.turso", "db--t--g.turso", "db--t--g.turso"}, f.hosts)
	assert.Equal(t, "Bearer tok", f.auth[0])
}

func TestExecuteDecodesErrorsAndRows(t *testing.T) {
	f := &fakeServer{}
	f.respond = func(req PipelineRequest) PipelineResponse {
		var first StreamResult
		if req.Requests[0].Stmt.SQL == "bad" {
			first = StreamResult{Type: "error", Error: &Error{Message: "no such table: x", Code: "SQLITE_ERROR"}}
		} else {
			name, decl := "n", "INTEGER"
			first = okExecute(t, StmtResult{Cols: []Col{{Name: &name, Decltype: &decl}}, Rows: [][]Value{{{Type: ValueInteger, Int: 7}}}})
		}
		return PipelineResponse{Results: []StreamResult{first, {Type: "ok", Response: &StreamResponse{Type: "close"}}}}
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	c := NewClient(Config{URL: srv.URL})

	res, err := c.Execute(context.Background(), Stmt{SQL: "SELECT 7 AS n", WantRows: true})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, int64(7), res.Rows[0][0].Int)

	_, err = c.Execute(context.Background(), Stmt{SQL: "bad"})
	var herr *Error
	require.ErrorAs(t, err, &herr)
	assert.Equal(t, "SQLITE_ERROR", herr.Code)
}

func TestHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "database not found", http.StatusNotFound)
	}))
	defer srv.Close()
	_, err := NewClient(Config{URL: srv.URL}).Execute(context.Background(), Stmt{SQL: "SELECT 1"})
	var herr *HTTPError
	require.ErrorAs(t, err, &herr)
	assert.Equal(t, http.StatusNotFound, herr.StatusCode)
	assert.Equal(t, "database not found", herr.Body)
}
