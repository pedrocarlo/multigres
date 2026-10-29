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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

func TestSetConfigResult(t *testing.T) {
	res, ok := setConfigResult(`SELECT pg_catalog.set_config('search_path', '', TRUE)`)
	require.True(t, ok)
	require.Len(t, res.Fields, 1)
	assert.Equal(t, "set_config", res.Fields[0].Name)
	assert.Equal(t, []sqltypes.Value{sqltypes.Value("")}, res.Rows[0].Values)

	res, ok = setConfigResult(`SELECT set_config('a.b', 'x', false) AS one, set_config('c.d', 'y', true)`)
	require.True(t, ok)
	assert.Equal(t, "one", res.Fields[0].Name)
	assert.Equal(t, []sqltypes.Value{sqltypes.Value("x"), sqltypes.Value("y")}, res.Rows[0].Values)

	for _, sql := range []string{
		`SELECT 1`,
		`SELECT set_config('a.b', current_user, true)`,
		`SELECT set_config('a.b', 'x', true) FROM t`,
		`SELECT set_config('a.b', 'x', true), 1`,
		`SELECT other.set_config('a', 'b', true)`,
	} {
		_, ok := setConfigResult(sql)
		assert.False(t, ok, sql)
	}
}

func TestBindArgs(t *testing.T) {
	// Lengths/values packing as the gateway sends them: -1 is NULL.
	portal := portalWith([]string{"42", "t", "1.5", "hello"}, true)
	args, err := bindArgs([]uint32{23, 16, 701, 0, 25}, portal)
	require.NoError(t, err)
	require.Len(t, args, 5)
	assert.Equal(t, int64(42), args[0].Int)
	assert.Equal(t, int64(1), args[1].Int)
	assert.InDelta(t, 1.5, args[2].Float, 1e-9)
	assert.Equal(t, "hello", args[3].Text)
	assert.Equal(t, "null", string(args[4].Type))

	_, err = bindArgs([]uint32{23}, portalWith([]string{"x"}, false))
	require.Error(t, err)
}

// portalWith returns a text-format portal with values, and a trailing NULL
// when withNull is set.
func portalWith(values []string, withNull bool) *query.Portal {
	params := make([][]byte, 0, len(values)+1)
	for _, v := range values {
		params = append(params, []byte(v))
	}
	if withNull {
		params = append(params, nil)
	}
	lengths, packed := sqltypes.ParamsToProto(params)
	return &query.Portal{ParamLengths: lengths, ParamValues: packed}
}
