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
	"strings"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// setConfigResult answers a SELECT made only of set_config calls with
// constant arguments, such as the gateway's probe for pg_dump's empty
// search_path: set_config with the session's value and TRUE
// (transaction-local).
//
// The gateway tracks session settings itself and probes the backend with a
// transaction-local set_config to learn the value; turso refuses
// transaction-local settings, and a Hrana statement has no session to keep a
// setting in anyway. set_config returns the new value, so the answer is the
// second argument.
func setConfigResult(sql string) (*sqltypes.Result, bool) {
	if !strings.Contains(strings.ToLower(sql), "set_config") {
		return nil, false
	}
	stmts, err := parser.ParseSQL(sql)
	if err != nil || len(stmts) != 1 {
		return nil, false
	}
	sel, ok := stmts[0].(*ast.SelectStmt)
	if !ok || sel.TargetList == nil || len(sel.TargetList.Items) == 0 || sel.FromClause != nil || sel.WhereClause != nil {
		return nil, false
	}
	var fields []*query.Field
	var values []sqltypes.Value
	for _, item := range sel.TargetList.Items {
		rt, ok := item.(*ast.ResTarget)
		if !ok {
			return nil, false
		}
		call, ok := rt.Val.(*ast.FuncCall)
		if !ok || !isSetConfig(call) || call.Args == nil || len(call.Args.Items) != 3 {
			return nil, false
		}
		value, ok := constString(call.Args.Items[1])
		if !ok {
			return nil, false
		}
		name := rt.Name
		if name == "" {
			name = "set_config"
		}
		fields = append(fields, &query.Field{Name: name, Type: "text", DataTypeOid: uint32(textOID), DataTypeSize: -1, TypeModifier: -1})
		values = append(values, sqltypes.Value(value))
	}
	return &sqltypes.Result{
		Fields:     fields,
		Rows:       []*sqltypes.Row{{Values: values}},
		CommandTag: "SELECT 1",
	}, true
}

func isSetConfig(call *ast.FuncCall) bool {
	if call.Funcname == nil {
		return false
	}
	items := call.Funcname.Items
	if len(items) == 0 || len(items) > 2 {
		return false
	}
	last, ok := items[len(items)-1].(*ast.String)
	if !ok || last.SVal != "set_config" {
		return false
	}
	if len(items) == 2 {
		schema, ok := items[0].(*ast.String)
		return ok && schema.SVal == "pg_catalog"
	}
	return true
}

func constString(n ast.Node) (string, bool) {
	c, ok := n.(*ast.A_Const)
	if !ok || c.Isnull {
		return "", false
	}
	s, ok := c.Val.(*ast.String)
	if !ok {
		return "", false
	}
	return s.SVal, true
}
