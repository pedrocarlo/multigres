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
	"fmt"
	"strconv"

	"github.com/multigres/multigres/go/common/hrana"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

const textOID = ast.TEXTOID

// bindArgs converts a portal's parameters to Hrana arguments. Only the text
// format is supported. Numeric and boolean parameters are sent as numbers,
// since turso stores booleans as 0/1; everything else is text.
func bindArgs(paramTypes []uint32, portal *query.Portal) ([]hrana.Value, error) {
	values := sqltypes.ParamsFromProto(portal.GetParamLengths(), portal.GetParamValues())
	formats := portal.GetParamFormats()
	args := make([]hrana.Value, len(values))
	for i, v := range values {
		format := int32(0)
		switch {
		case len(formats) == 1:
			format = formats[0]
		case i < len(formats):
			format = formats[i]
		}
		if format != 0 {
			return nil, notSupported("binary parameters are not supported on turso-server")
		}
		if v == nil {
			args[i] = hrana.Null()
			continue
		}
		oid := ast.Oid(0)
		if i < len(paramTypes) {
			oid = ast.Oid(paramTypes[i])
		}
		arg, err := textArg(oid, string(v))
		if err != nil {
			return nil, mterrors.NewPgError("ERROR", "22P02", err.Error(), "")
		}
		args[i] = arg
	}
	return args, nil
}

func textArg(oid ast.Oid, s string) (hrana.Value, error) {
	switch oid {
	case ast.INT2OID, ast.INT4OID, ast.INT8OID, ast.OIDOID:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return hrana.Value{}, fmt.Errorf("invalid input syntax for type integer: %q", s)
		}
		return hrana.Value{Type: hrana.ValueInteger, Int: i}, nil
	case ast.FLOAT4OID, ast.FLOAT8OID:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return hrana.Value{}, fmt.Errorf("invalid input syntax for type double precision: %q", s)
		}
		return hrana.Value{Type: hrana.ValueFloat, Float: f}, nil
	case ast.BOOLOID:
		b, ok := sqltypes.ParseBool(s)
		if !ok {
			return hrana.Value{}, fmt.Errorf("invalid input syntax for type boolean: %q", s)
		}
		if b {
			return hrana.Value{Type: hrana.ValueInteger, Int: 1}, nil
		}
		return hrana.Value{Type: hrana.ValueInteger, Int: 0}, nil
	}
	return hrana.Text(s), nil
}

// checkResultFormats rejects binary result columns: Hrana values are sent in
// text form only.
func checkResultFormats(formats []int32) error {
	for _, f := range formats {
		if f != 0 {
			return notSupported("binary results are not supported on turso-server")
		}
	}
	return nil
}
