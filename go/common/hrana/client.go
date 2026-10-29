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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// maxErrorBody bounds how much of a non-2xx response body is kept in errors.
const maxErrorBody = 4096

// Config configures a Client.
type Config struct {
	// URL is turso-server's user API (or db-router), for example
	// http://127.0.0.1:8080.
	URL string
	// Host, when set, is sent as the HTTP Host header. turso-server and
	// db-router pick the database from it: {db}--{tenant}--{group}.turso.
	Host string
	// Token, when set, returns the bearer token for each request. It is called
	// per request so a rotated token is picked up.
	Token func() (string, error)
	// HTTPClient sends the requests; http.DefaultClient when nil.
	HTTPClient *http.Client
}

// Client sends Hrana pipelines to one database.
type Client struct {
	cfg Config
}

// NewClient returns a client for cfg.
func NewClient(cfg Config) *Client {
	cfg.URL = strings.TrimRight(cfg.URL, "/")
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	return &Client{cfg: cfg}
}

// HTTPError is a non-2xx answer from the server.
type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("hrana: HTTP %d: %s", e.StatusCode, e.Body)
}

// pipeline sends one pipeline request to baseURL.
func (c *Client) pipeline(ctx context.Context, baseURL string, req *PipelineRequest) (*PipelineResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("hrana: encode pipeline: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v3/pipeline", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("hrana: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.cfg.Host != "" {
		httpReq.Host = c.cfg.Host
	}
	if c.cfg.Token != nil {
		token, err := c.cfg.Token()
		if err != nil {
			return nil, fmt.Errorf("hrana: read token: %w", err)
		}
		if token != "" {
			httpReq.Header.Set("Authorization", "Bearer "+token)
		}
	}
	resp, err := c.cfg.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("hrana: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return nil, &HTTPError{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	var out PipelineResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("hrana: decode pipeline: %w", err)
	}
	if len(out.Results) != len(req.Requests) {
		return nil, fmt.Errorf("hrana: %d results for %d requests", len(out.Results), len(req.Requests))
	}
	return &out, nil
}

// Stream is a Hrana stream: a server-side connection kept between pipelines
// by the baton. A transaction lives on one stream. A Stream is not safe for
// concurrent use; the server orders pipelines by baton anyway.
type Stream struct {
	c       *Client
	mu      sync.Mutex
	baton   *string
	baseURL string
	closed  bool
}

// NewStream returns a stream that opens on its first pipeline.
func (c *Client) NewStream() *Stream {
	return &Stream{c: c, baseURL: c.cfg.URL}
}

// ErrStreamClosed is returned for a pipeline on a closed stream.
var ErrStreamClosed = errors.New("hrana: stream is closed")

// Pipeline runs requests on the stream. A transport error closes the stream:
// its baton cannot be trusted any more.
func (s *Stream) Pipeline(ctx context.Context, requests ...StreamRequest) ([]StreamResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStreamClosed
	}
	resp, err := s.c.pipeline(ctx, s.baseURL, &PipelineRequest{Baton: s.baton, Requests: requests})
	if err != nil {
		s.closed = true
		return nil, err
	}
	s.baton = resp.Baton
	if resp.BaseURL != nil && *resp.BaseURL != "" {
		s.baseURL = strings.TrimRight(*resp.BaseURL, "/")
	}
	if resp.Baton == nil {
		s.closed = true
	}
	return resp.Results, nil
}

// Close closes the stream on the server. A stream that never ran a pipeline
// or is already closed needs no request.
func (s *Stream) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed || s.baton == nil {
		s.closed = true
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	_, err := s.Pipeline(ctx, CloseRequest())
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return err
}

// Execute runs one statement on a new stream and closes it in the same
// pipeline (one round trip, autocommit).
func (c *Client) Execute(ctx context.Context, stmt Stmt) (*StmtResult, error) {
	s := c.NewStream()
	results, err := s.Pipeline(ctx, ExecuteRequest(stmt), CloseRequest())
	if err != nil {
		return nil, err
	}
	return DecodeExecute(results[0])
}

// DecodeExecute returns the statement result of an execute request, or its
// error.
func DecodeExecute(r StreamResult) (*StmtResult, error) {
	raw, err := okResult(r, "execute")
	if err != nil {
		return nil, err
	}
	var out StmtResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("hrana: decode execute result: %w", err)
	}
	return &out, nil
}

// DecodeDescribe returns the result of a describe request, or its error.
func DecodeDescribe(r StreamResult) (*DescribeResult, error) {
	raw, err := okResult(r, "describe")
	if err != nil {
		return nil, err
	}
	var out DescribeResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("hrana: decode describe result: %w", err)
	}
	return &out, nil
}

func okResult(r StreamResult, want string) (json.RawMessage, error) {
	switch r.Type {
	case "ok":
		if r.Response == nil || r.Response.Type != want {
			return nil, fmt.Errorf("hrana: expected a %s response", want)
		}
		return r.Response.Result, nil
	case "error":
		if r.Error == nil {
			return nil, errors.New("hrana: error without details")
		}
		return nil, r.Error
	default:
		return nil, fmt.Errorf("hrana: request did not run (%s)", r.Type)
	}
}
