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

package hranapooler

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/hrana"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/sqltypes"
)

func TestTypeOfDecl(t *testing.T) {
	tests := []struct {
		decl *string
		want ast.Oid
	}{
		{nil, ast.TEXTOID},
		{new("INTEGER"), ast.INT4OID},
		{new("BIGINT"), ast.INT8OID},
		{new("BOOLEAN"), ast.BOOLOID},
		{new("bool"), ast.BOOLOID},
		{new("REAL"), ast.FLOAT4OID},
		{new("DOUBLE"), ast.FLOAT8OID},
		{new("varchar(32)"), ast.VARCHAROID},
		{new("numeric(10,2)"), ast.NUMERICOID},
		{new("BLOB"), ast.BYTEAOID},
		{new("timestamptz"), ast.TIMESTAMPTZOID},
		{new("TEXT[]"), ast.TEXTARRAYOID},
		{new("INTEGER[]"), ast.INT4ARRAYOID},
		{new("whatever"), ast.TEXTOID},
	}
	for _, tt := range tests {
		name := "<nil>"
		if tt.decl != nil {
			name = *tt.decl
		}
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.want, typeOfDecl(tt.decl).oid)
		})
	}
}

func TestTextValue(t *testing.T) {
	boolType := columnType{oid: ast.BOOLOID}
	textType := columnType{oid: ast.TEXTOID}
	tests := []struct {
		name string
		v    hrana.Value
		t    columnType
		want sqltypes.Value
	}{
		{"null", hrana.Null(), textType, nil},
		{"integer", hrana.Value{Type: hrana.ValueInteger, Int: -42}, textType, sqltypes.Value("-42")},
		{"bool true", hrana.Value{Type: hrana.ValueInteger, Int: 1}, boolType, sqltypes.Value("t")},
		{"bool false", hrana.Value{Type: hrana.ValueInteger, Int: 0}, boolType, sqltypes.Value("f")},
		{"float", hrana.Value{Type: hrana.ValueFloat, Float: 0.1}, textType, sqltypes.Value("0.1")},
		{"inf", hrana.Value{Type: hrana.ValueFloat, Float: math.Inf(-1)}, textType, sqltypes.Value("-Infinity")},
		{"text", hrana.Text("x"), textType, sqltypes.Value("x")},
		{"empty text", hrana.Text(""), textType, sqltypes.Value("")},
		{"timestamptz", hrana.Text("2026-01-01 00:00:00"), columnType{oid: ast.TIMESTAMPTZOID}, sqltypes.Value("2026-01-01 00:00:00+00")},
		{"timestamptz with zone", hrana.Text("2026-01-01 00:00:00+02"), columnType{oid: ast.TIMESTAMPTZOID}, sqltypes.Value("2026-01-01 00:00:00+02")},
		{"blob", hrana.Value{Type: hrana.ValueBlob, Blob: []byte{0xde, 0xad}}, textType, sqltypes.Value(`\xdead`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := textValue(tt.v, tt.t)
			if tt.want == nil {
				assert.Nil(t, got)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestResultOf(t *testing.T) {
	t.Run("query", func(t *testing.T) {
		res := resultOf("SELECT relhasindex, relname FROM pg_class", &hrana.StmtResult{
			Cols: []hrana.Col{{Name: new("relhasindex"), Decltype: new("BOOLEAN")}, {Name: new("relname"), Decltype: new("TEXT")}},
			Rows: [][]hrana.Value{{{Type: hrana.ValueInteger, Int: 1}, hrana.Text("todos")}},
		})
		assert.Equal(t, "SELECT 1", res.CommandTag)
		require.Len(t, res.Fields, 2)
		assert.Equal(t, uint32(ast.BOOLOID), res.Fields[0].DataTypeOid)
		assert.Equal(t, []sqltypes.Value{sqltypes.Value("t"), sqltypes.Value("todos")}, res.Rows[0].Values)
	})
	t.Run("placeholder column of a non-query is dropped", func(t *testing.T) {
		res := resultOf("LOCK TABLE public.todos IN ACCESS SHARE MODE", &hrana.StmtResult{Cols: []hrana.Col{{Name: new("0")}}})
		assert.Equal(t, "LOCK TABLE", res.CommandTag)
		assert.Nil(t, res.Fields)
	})
	t.Run("insert", func(t *testing.T) {
		res := resultOf("INSERT INTO t VALUES (1), (2)", &hrana.StmtResult{AffectedRowCount: 2})
		assert.Equal(t, "INSERT 0 2", res.CommandTag)
		assert.Nil(t, res.Fields)
	})
	t.Run("insert returning", func(t *testing.T) {
		res := resultOf("INSERT INTO t VALUES (1) RETURNING id", &hrana.StmtResult{
			Cols: []hrana.Col{{Name: new("id")}}, Rows: [][]hrana.Value{{{Type: hrana.ValueInteger, Int: 1}}}, AffectedRowCount: 1,
		})
		assert.Equal(t, "INSERT 0 1", res.CommandTag)
		assert.Len(t, res.Fields, 1)
	})
}

func TestCommandTag(t *testing.T) {
	tests := map[string]string{
		"select 1":                             "SELECT 0",
		"  -- c\n/* x */ (SELECT 1)":           "SELECT 0",
		"UPDATE t SET a = 1":                   "UPDATE 3",
		"DELETE FROM t":                        "DELETE 3",
		"CREATE UNIQUE INDEX i ON t (a)":       "CREATE INDEX",
		"CREATE OR REPLACE VIEW v AS SELECT 1": "CREATE VIEW",
		"create materialized view m as select": "CREATE MATERIALIZED VIEW",
		"DROP TABLE t":                         "DROP TABLE",
		"BEGIN":                                "BEGIN",
		"commit":                               "COMMIT",
		"END":                                  "COMMIT",
		"SET search_path = ''":                 "SET",
		"COMMENT ON TABLE t IS 'x'":            "COMMENT",
	}
	for sql, want := range tests {
		t.Run(sql, func(t *testing.T) {
			assert.Equal(t, want, commandTag(leadingWords(sql, 4), false, 0, 3))
		})
	}
}

func TestPgError(t *testing.T) {
	tests := []struct {
		err  error
		code string
	}{
		{&hrana.Error{Message: "UNIQUE constraint failed: t.id", Code: "SQLITE_CONSTRAINT"}, "23505"},
		{&hrana.Error{Message: "NOT NULL constraint failed: t.v", Code: "SQLITE_CONSTRAINT"}, "23502"},
		{&hrana.Error{Message: "no such table: x", Code: "SQLITE_ERROR"}, "42P01"},
		{&hrana.Error{Message: "SQL string could not be parsed: CREATE POLICY is not supported", Code: "SQL_PARSE_ERROR"}, "0A000"},
		{&hrana.Error{Message: "syntax error", Code: "SQL_PARSE_ERROR"}, "42601"},
		{&hrana.Error{Message: "boom", Code: "SQLITE_UNKNOWN"}, "XX000"},
		{&hrana.HTTPError{StatusCode: 404}, "3D000"},
		{&hrana.HTTPError{StatusCode: 503}, "08006"},
		{errors.New("dial tcp: connection refused"), "08006"},
	}
	for _, tt := range tests {
		t.Run(tt.err.Error(), func(t *testing.T) {
			var diag *mterrors.PgDiagnostic
			require.ErrorAs(t, pgError(tt.err), &diag)
			assert.Equal(t, tt.code, diag.SQLSTATE())
		})
	}
}
