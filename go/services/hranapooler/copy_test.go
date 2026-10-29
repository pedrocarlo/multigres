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
)

func TestCopyToSelect(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		// What pg_dump sends.
		{`COPY public.todos (id, title, done) TO stdout;`, `SELECT id, title, done FROM public.todos`},
		{`COPY public."Mixed Case" ("Col", other) TO STDOUT`, `SELECT "Col", other FROM public."Mixed Case"`},
		{`COPY t TO STDOUT`, `SELECT * FROM t`},
		{`COPY t TO STDOUT (FORMAT text)`, `SELECT * FROM t`},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			got, err := copyToSelect(tt.sql)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	for _, sql := range []string{
		`COPY t FROM STDIN`,
		`COPY t TO '/tmp/x'`,
		`COPY t TO STDOUT (FORMAT csv)`,
		`COPY t TO STDOUT (FORMAT binary)`,
	} {
		t.Run("rejects "+sql, func(t *testing.T) {
			_, err := copyToSelect(sql)
			require.Error(t, err)
		})
	}
}

func TestAppendCopyRow(t *testing.T) {
	row := &sqltypes.Row{Values: []sqltypes.Value{
		sqltypes.Value("1"),
		nil,
		sqltypes.Value("a\tb\nc\\d\re"),
		sqltypes.Value(""),
	}}
	assert.Equal(t, "1\t\\N\ta\\tb\\nc\\\\d\\re\t\n", string(appendCopyRow(nil, row)))
}
