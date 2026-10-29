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
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/hrana"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// columnType is the PostgreSQL type of a result column, as far as Hrana
// tells: its declared type (decltype). turso's own wire server maps
// declared types the same way (postgres/server sqlite_type_to_pg_type).
type columnType struct {
	oid  ast.Oid
	name string
}

// typeOfDecl maps a column's declared type to a PostgreSQL type. Columns
// without one (expressions) and unknown types are text: the value is sent
// in text form either way.
func typeOfDecl(decl *string) columnType {
	if decl == nil || *decl == "" {
		return columnType{oid: ast.TEXTOID, name: "text"}
	}
	name := strings.ToLower(strings.TrimSpace(*decl))
	if base, ok := strings.CutSuffix(name, "[]"); ok {
		elem := typeOfDecl(&base)
		if arr := ast.ArrayTypeOid(elem.oid); arr != ast.InvalidOid {
			return columnType{oid: arr, name: "_" + elem.name}
		}
		return columnType{oid: ast.TEXTARRAYOID, name: "_text"}
	}
	// Drop a type modifier: varchar(50), numeric(10,2).
	if i := strings.IndexByte(name, '('); i >= 0 {
		name = strings.TrimSpace(name[:i])
	}
	switch name {
	case "serial", "smallserial":
		return columnType{oid: ast.INT4OID, name: "int4"}
	case "bigserial":
		return columnType{oid: ast.INT8OID, name: "int8"}
	case "double", "float":
		return columnType{oid: ast.FLOAT8OID, name: "float8"}
	case "blob":
		return columnType{oid: ast.BYTEAOID, name: "bytea"}
	case "character":
		return columnType{oid: ast.BPCHAROID, name: "bpchar"}
	}
	if oid := ast.TypeNameToOid(name); oid != ast.InvalidOid {
		return columnType{oid: oid, name: name}
	}
	return columnType{oid: ast.TEXTOID, name: "text"}
}

// fieldsOf returns the RowDescription of Hrana columns.
func fieldsOf(cols []hrana.Col) ([]*query.Field, []columnType) {
	fields := make([]*query.Field, len(cols))
	types := make([]columnType, len(cols))
	for i, col := range cols {
		t := typeOfDecl(col.Decltype)
		name := "?column?"
		if col.Name != nil && *col.Name != "" {
			name = *col.Name
		}
		types[i] = t
		fields[i] = &query.Field{
			Name:                 name,
			Type:                 t.name,
			DataTypeOid:          uint32(t.oid),
			DataTypeSize:         -1,
			TypeModifier:         -1,
			TableAttributeNumber: 0,
		}
	}
	return fields, types
}

// textValue renders a Hrana value in PostgreSQL's text format for a column
// of type t. NULL is nil.
func textValue(v hrana.Value, t columnType) sqltypes.Value {
	switch v.Type {
	case hrana.ValueNull:
		return nil
	case hrana.ValueInteger:
		// turso stores booleans as 0/1.
		if t.oid == ast.BOOLOID {
			if v.Int != 0 {
				return sqltypes.Value("t")
			}
			return sqltypes.Value("f")
		}
		return sqltypes.Value(strconv.FormatInt(v.Int, 10))
	case hrana.ValueFloat:
		return sqltypes.Value(formatFloat(v.Float))
	case hrana.ValueText:
		if t.oid == ast.TIMESTAMPTZOID && !strings.ContainsAny(v.Text, "+Z") && !strings.HasSuffix(v.Text, "-00") {
			return sqltypes.Value(v.Text + "+00")
		}
		return sqltypes.Value(v.Text)
	case hrana.ValueBlob:
		// bytea's text form is hex.
		out := make([]byte, 2+hex.EncodedLen(len(v.Blob)))
		out[0], out[1] = '\\', 'x'
		hex.Encode(out[2:], v.Blob)
		return sqltypes.Value(out)
	default:
		return nil
	}
}

// formatFloat renders a float the way PostgreSQL's float8 output does
// (shortest exact form, Infinity, NaN).
func formatFloat(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// rowsOf renders Hrana rows in text format.
func rowsOf(rows [][]hrana.Value, types []columnType) []*sqltypes.Row {
	out := make([]*sqltypes.Row, len(rows))
	for i, row := range rows {
		values := make([]sqltypes.Value, len(row))
		for j, v := range row {
			t := columnType{oid: ast.TEXTOID}
			if j < len(types) {
				t = types[j]
			}
			values[j] = textValue(v, t)
		}
		out[i] = &sqltypes.Row{Values: values}
	}
	return out
}

// resultOf converts the Hrana result of sql to a query result.
func resultOf(sql string, r *hrana.StmtResult) *sqltypes.Result {
	words := leadingWords(sql, 4)
	// turso answers some statements that return no rows (LOCK, SET, COMMENT,
	// ...) with a placeholder column; PostgreSQL sends no RowDescription for
	// them, and clients such as pg_dump check for that.
	hasRows := len(r.Cols) > 0 && returnsRows(words)
	tag := commandTag(words, hasRows, len(r.Rows), r.AffectedRowCount)
	if !hasRows {
		return &sqltypes.Result{CommandTag: tag, RowsAffected: r.AffectedRowCount}
	}
	fields, types := fieldsOf(r.Cols)
	return &sqltypes.Result{
		Fields:       fields,
		Rows:         rowsOf(r.Rows, types),
		RowsAffected: r.AffectedRowCount,
		CommandTag:   tag,
	}
}

// returnsRows reports whether a statement starting with words can return
// rows: a query, or DML (with RETURNING, when turso sends columns).
func returnsRows(words []string) bool {
	if len(words) == 0 {
		return false
	}
	switch words[0] {
	case "SELECT", "VALUES", "TABLE", "WITH", "SHOW", "EXPLAIN", "FETCH", "INSERT", "UPDATE", "DELETE", "MERGE":
		return true
	}
	return false
}

// commandTag returns PostgreSQL's command tag for a statement starting with
// words. Hrana reports no tag, so it is derived from the leading keywords.
func commandTag(words []string, hasRows bool, rowCount int, affected uint64) string {
	if len(words) == 0 {
		return ""
	}
	first := words[0]
	switch first {
	case "SELECT", "VALUES", "TABLE", "WITH", "EXPLAIN", "FETCH":
		return fmt.Sprintf("SELECT %d", rowCount)
	case "SHOW":
		return "SHOW"
	case "INSERT":
		n := affected
		if hasRows {
			n = uint64(rowCount)
		}
		return fmt.Sprintf("INSERT 0 %d", n)
	case "UPDATE", "DELETE", "MERGE", "MOVE", "COPY":
		n := affected
		if hasRows {
			n = uint64(rowCount)
		}
		return fmt.Sprintf("%s %d", first, n)
	case "START":
		return "START TRANSACTION"
	case "END":
		return "COMMIT"
	case "ABORT":
		return "ROLLBACK"
	case "LOCK":
		return "LOCK TABLE"
	case "CREATE", "DROP", "ALTER":
		return first + " " + objectKind(words[1:])
	}
	return first
}

// objectKind names the object of CREATE/DROP/ALTER for the command tag:
// CREATE UNIQUE INDEX is "CREATE INDEX", CREATE OR REPLACE VIEW is
// "CREATE VIEW".
func objectKind(words []string) string {
	for _, w := range words {
		switch w {
		case "OR", "REPLACE", "UNIQUE", "TEMP", "TEMPORARY", "UNLOGGED", "GLOBAL", "LOCAL":
			continue
		case "MATERIALIZED":
			return "MATERIALIZED VIEW"
		}
		return w
	}
	return ""
}

// leadingWords returns up to n leading keywords of sql, upper-cased, after
// comments and an opening parenthesis.
func leadingWords(sql string, n int) []string {
	s := skipSpaceAndComments(sql)
	s = strings.TrimLeft(s, "( \t\r\n")
	var words []string
	for len(words) < n {
		s = skipSpaceAndComments(s)
		end := 0
		for end < len(s) && (s[end] == '_' || s[end] >= 'a' && s[end] <= 'z' || s[end] >= 'A' && s[end] <= 'Z') {
			end++
		}
		if end == 0 {
			break
		}
		words = append(words, strings.ToUpper(s[:end]))
		s = s[end:]
	}
	return words
}

func skipSpaceAndComments(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		switch {
		case strings.HasPrefix(s, "--"):
			i := strings.IndexByte(s, '\n')
			if i < 0 {
				return ""
			}
			s = s[i+1:]
		case strings.HasPrefix(s, "/*"):
			i := strings.Index(s, "*/")
			if i < 0 {
				return ""
			}
			s = s[i+2:]
		default:
			return s
		}
	}
}

// pgError converts a Hrana or transport error to a PostgreSQL error. turso
// sends no SQLSTATE, so the common cases are mapped from its code and
// message; everything else is XX000 with turso's message.
func pgError(err error) error {
	if err == nil {
		return nil
	}
	var diag *mterrors.PgDiagnostic
	if errors.As(err, &diag) {
		return err
	}
	var herr *hrana.Error
	if errors.As(err, &herr) {
		return mterrors.NewPgError("ERROR", sqlstateOf(herr), herr.Message, "")
	}
	var httpErr *hrana.HTTPError
	if errors.As(err, &httpErr) {
		if httpErr.StatusCode == http.StatusNotFound {
			return mterrors.NewPgError("FATAL", "3D000", "database does not exist on turso-server", httpErr.Body)
		}
		return mterrors.NewPgError("ERROR", "08006", fmt.Sprintf("turso-server answered HTTP %d", httpErr.StatusCode), httpErr.Body)
	}
	return mterrors.NewPgError("ERROR", "08006", "cannot reach turso-server", err.Error())
}

func sqlstateOf(e *hrana.Error) string {
	msg := e.Message
	switch {
	case strings.Contains(msg, "UNIQUE constraint failed"):
		return "23505"
	case strings.Contains(msg, "NOT NULL constraint failed"):
		return "23502"
	case strings.Contains(msg, "FOREIGN KEY constraint failed"):
		return "23503"
	case strings.Contains(msg, "CHECK constraint failed"):
		return "23514"
	case strings.Contains(msg, "no such table"), strings.Contains(msg, "does not exist") && strings.Contains(msg, "relation"):
		return "42P01"
	case strings.Contains(msg, "no such column"):
		return "42703"
	case strings.Contains(msg, "no such function"):
		return "42883"
	case strings.Contains(msg, "is not supported"), strings.Contains(msg, "not allowed"):
		return "0A000"
	case e.Code == "SQL_PARSE_ERROR":
		return "42601"
	}
	return "XX000"
}
