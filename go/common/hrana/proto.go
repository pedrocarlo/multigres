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

// Package hrana is a client for Hrana v3 over HTTP (`POST /v3/pipeline`), the
// SQL-over-HTTP protocol of turso-server. It holds only the messages the
// Hrana pooler sends and reads; names follow turso-server's
// `server/src/proto.rs`.
package hrana

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
)

// PipelineRequest is the body of `POST /v3/pipeline`.
type PipelineRequest struct {
	// Baton continues a stream; nil opens a new one.
	Baton *string `json:"baton"`
	// Requests run in order on the stream.
	Requests []StreamRequest `json:"requests"`
}

// PipelineResponse is the response of `POST /v3/pipeline`.
type PipelineResponse struct {
	// Baton continues the stream; nil once the stream is closed.
	Baton *string `json:"baton"`
	// BaseURL, when set, is where the next request of the stream must go.
	BaseURL *string `json:"base_url"`
	// Results holds one result per request.
	Results []StreamResult `json:"results"`
}

// StreamRequest is one request of a pipeline.
type StreamRequest struct {
	Type string `json:"type"`
	Stmt *Stmt  `json:"stmt,omitempty"`
	SQL  string `json:"sql,omitempty"`
}

// ExecuteRequest runs one statement.
func ExecuteRequest(stmt Stmt) StreamRequest {
	return StreamRequest{Type: "execute", Stmt: &stmt}
}

// DescribeRequest describes the parameters and columns of a statement
// without running it.
func DescribeRequest(sql string) StreamRequest {
	return StreamRequest{Type: "describe", SQL: sql}
}

// CloseRequest closes the stream; an open transaction rolls back.
func CloseRequest() StreamRequest {
	return StreamRequest{Type: "close"}
}

// Stmt is one statement with positional arguments ($1 is Args[0]).
type Stmt struct {
	SQL      string  `json:"sql"`
	Args     []Value `json:"args,omitempty"`
	WantRows bool    `json:"want_rows"`
}

// StreamResult is the result of one pipeline request.
type StreamResult struct {
	// Type is "ok", "error" or "none" (did not run: an earlier request failed).
	Type     string          `json:"type"`
	Response *StreamResponse `json:"response,omitempty"`
	Error    *Error          `json:"error,omitempty"`
}

// StreamResponse is a successful response. Result is decoded by the caller,
// by Type: StmtResult for "execute", DescribeResult for "describe".
type StreamResponse struct {
	Type   string          `json:"type"`
	Result json.RawMessage `json:"result,omitempty"`
}

// StmtResult is the result of one statement.
type StmtResult struct {
	Cols             []Col     `json:"cols"`
	Rows             [][]Value `json:"rows"`
	AffectedRowCount uint64    `json:"affected_row_count"`
}

// DescribeResult is the result of a describe request.
type DescribeResult struct {
	Params     []DescribeParam `json:"params"`
	Cols       []Col           `json:"cols"`
	IsExplain  bool            `json:"is_explain"`
	IsReadonly bool            `json:"is_readonly"`
}

// DescribeParam is one parameter of a described statement.
type DescribeParam struct {
	Name *string `json:"name"`
}

// Col is a result column.
type Col struct {
	Name     *string `json:"name"`
	Decltype *string `json:"decltype"`
}

// Error is a Hrana error. Code is turso's code (for example SQLITE_CONSTRAINT),
// not a SQLSTATE: turso-server reports none.
type Error struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

func (e *Error) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

// ValueType is the type tag of a Hrana value.
type ValueType string

const (
	ValueNull    ValueType = "null"
	ValueInteger ValueType = "integer"
	ValueFloat   ValueType = "float"
	ValueText    ValueType = "text"
	ValueBlob    ValueType = "blob"
)

// Value is a Hrana value. Integers travel as decimal strings, floats as JSON
// numbers, blobs as base64.
type Value struct {
	Type ValueType
	// Int is set for integers.
	Int int64
	// Float is set for floats.
	Float float64
	// Text is set for text.
	Text string
	// Blob is set for blobs.
	Blob []byte
}

// Null is the NULL value.
func Null() Value { return Value{Type: ValueNull} }

// Text returns a text value.
func Text(s string) Value { return Value{Type: ValueText, Text: s} }

type wireValue struct {
	Type   ValueType       `json:"type"`
	Value  json.RawMessage `json:"value,omitempty"`
	Base64 string          `json:"base64,omitempty"`
}

// MarshalJSON encodes the value in Hrana's JSON form.
func (v Value) MarshalJSON() ([]byte, error) {
	w := wireValue{Type: v.Type}
	var err error
	switch v.Type {
	case ValueNull:
	case ValueInteger:
		w.Value, err = json.Marshal(strconv.FormatInt(v.Int, 10))
	case ValueFloat:
		w.Value, err = json.Marshal(v.Float)
	case ValueText:
		w.Value, err = json.Marshal(v.Text)
	case ValueBlob:
		w.Base64 = base64.StdEncoding.EncodeToString(v.Blob)
	default:
		return nil, fmt.Errorf("hrana: unknown value type %q", v.Type)
	}
	if err != nil {
		return nil, err
	}
	return json.Marshal(w)
}

// UnmarshalJSON decodes a value from Hrana's JSON form.
func (v *Value) UnmarshalJSON(data []byte) error {
	var w wireValue
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*v = Value{Type: w.Type}
	switch w.Type {
	case ValueNull:
	case ValueInteger:
		// Integers are strings, but accept numbers too.
		var s string
		if err := json.Unmarshal(w.Value, &s); err != nil {
			s = string(w.Value)
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("hrana: invalid integer %q", s)
		}
		v.Int = i
	case ValueFloat:
		if err := json.Unmarshal(w.Value, &v.Float); err != nil {
			return fmt.Errorf("hrana: invalid float: %w", err)
		}
	case ValueText:
		if err := json.Unmarshal(w.Value, &v.Text); err != nil {
			return fmt.Errorf("hrana: invalid text: %w", err)
		}
	case ValueBlob:
		b, err := decodeBase64(w.Base64)
		if err != nil {
			return fmt.Errorf("hrana: invalid blob: %w", err)
		}
		v.Blob = b
	default:
		return fmt.Errorf("hrana: unknown value type %q", w.Type)
	}
	return nil
}

// decodeBase64 accepts base64 with or without padding.
func decodeBase64(s string) ([]byte, error) {
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.RawStdEncoding.DecodeString(s)
}
