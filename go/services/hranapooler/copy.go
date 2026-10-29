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
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/hrana"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// copyChunkSize is the size of the CopyData chunks sent to the gateway.
const copyChunkSize = 64 << 10

// CopyBidiExecute serves COPY ... TO STDOUT. Hrana has no COPY: the COPY is
// rewritten to the SELECT it stands for, and the rows are encoded in COPY
// text format here. COPY FROM STDIN is not supported.
func (s *Server) CopyBidiExecute(stream multipoolerpb.MultipoolerService_CopyBidiExecuteServer) error {
	ctx := stream.Context()
	req, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to receive INITIATE: %v", err)
	}
	if req.GetPhase() != multipoolerpb.CopyBidiExecuteRequest_INITIATE {
		return status.Errorf(codes.InvalidArgument, "expected INITIATE, got %v", req.GetPhase())
	}
	rc, begin, err := s.connFor(ctx, req.GetOptions(), req.GetReservationOptions())
	if err != nil {
		return s.copyError(stream, nil, err)
	}
	// The gateway keeps every COPY on a reserved connection, an autocommit
	// one included; the COPY reason is dropped again when the COPY ends.
	if rc == nil {
		rc = s.newReserved(protoutil.ReasonCopy)
	} else {
		rc.reasons |= protoutil.ReasonCopy
	}

	res, err := s.copyRows(ctx, rc, begin, req)
	if err != nil {
		s.endCopy(ctx, rc)
		return s.copyError(stream, rc, err)
	}
	if err := sendCopyOut(stream, s.state(rc), res); err != nil {
		s.endCopy(ctx, rc)
		return err
	}
	s.endCopy(ctx, rc)
	return stream.Send(&multipoolerpb.CopyBidiExecuteResponse{
		Phase: multipoolerpb.CopyBidiExecuteResponse_RESULT,
		Result: &query.QueryResult{
			CommandTag:   fmt.Sprintf("COPY %d", len(res.Rows)),
			RowsAffected: uint64(len(res.Rows)),
		},
		ReservedState: s.state(rc),
	})
}

// copyRows runs the SELECT a COPY TO STDOUT reads.
func (s *Server) copyRows(ctx context.Context, rc *reservedConn, begin bool, req *multipoolerpb.CopyBidiExecuteRequest) (*sqltypes.Result, error) {
	if req.GetDirection() != multipoolerpb.CopyBidiExecuteRequest_TO_STDOUT {
		return nil, notSupported("COPY FROM STDIN is not supported on turso-server")
	}
	selectSQL, err := copyToSelect(req.GetQuery())
	if err != nil {
		return nil, err
	}
	return s.run(ctx, rc, begin, hrana.Stmt{SQL: selectSQL, WantRows: true})
}

// sendCopyOut sends READY and the rows as CopyData chunks.
func sendCopyOut(stream multipoolerpb.MultipoolerService_CopyBidiExecuteServer, state *query.ReservedState, res *sqltypes.Result) error {
	if err := stream.Send(&multipoolerpb.CopyBidiExecuteResponse{
		Phase:         multipoolerpb.CopyBidiExecuteResponse_READY,
		ReservedState: state,
		ColumnFormats: make([]int32, len(res.Fields)),
	}); err != nil {
		return status.Errorf(codes.Internal, "failed to send READY: %v", err)
	}
	var buf []byte
	for i, row := range res.Rows {
		buf = appendCopyRow(buf, row)
		if len(buf) >= copyChunkSize || i == len(res.Rows)-1 {
			if err := stream.Send(&multipoolerpb.CopyBidiExecuteResponse{Phase: multipoolerpb.CopyBidiExecuteResponse_DATA, Data: buf}); err != nil {
				return err
			}
			buf = nil
		}
	}
	return nil
}

// endCopy drops the COPY reason, releasing the connection when nothing else
// keeps it.
func (s *Server) endCopy(ctx context.Context, rc *reservedConn) {
	rc.reasons &^= protoutil.ReasonCopy
	if rc.reasons == 0 {
		s.release(ctx, rc)
	}
}

// copyError reports err on the COPY stream, with the reservation state that
// remains, and returns it as the RPC's error.
func (s *Server) copyError(stream multipoolerpb.MultipoolerService_CopyBidiExecuteServer, rc *reservedConn, err error) error {
	resp := &multipoolerpb.CopyBidiExecuteResponse{
		Phase:         multipoolerpb.CopyBidiExecuteResponse_ERROR,
		Error:         err.Error(),
		ReservedState: s.state(rc),
	}
	var diag *mterrors.PgDiagnostic
	if errors.As(err, &diag) {
		resp.ErrorDiagnostic = mterrors.PgDiagnosticToProto(diag)
	}
	_ = stream.Send(resp)
	return mterrors.ToGRPC(err)
}

// copyToSelect returns the SELECT a COPY ... TO STDOUT reads. Only the text
// format is supported, which is what pg_dump uses.
func copyToSelect(sql string) (string, error) {
	stmts, err := parser.ParseSQL(sql)
	if err != nil {
		return "", mterrors.NewPgError("ERROR", "42601", err.Error(), "")
	}
	if len(stmts) != 1 {
		return "", mterrors.NewPgError("ERROR", "42601", "expected one COPY statement", "")
	}
	cp, ok := stmts[0].(*ast.CopyStmt)
	if !ok || cp.IsFrom || cp.IsProgram || cp.Filename != "" {
		return "", notSupported("only COPY ... TO STDOUT is supported on turso-server")
	}
	if cp.Options != nil {
		for _, item := range cp.Options.Items {
			def, ok := item.(*ast.DefElem)
			if !ok {
				continue
			}
			if def.Defname != "format" || !strings.EqualFold(defString(def), "text") {
				return "", notSupported(fmt.Sprintf("COPY option %q is not supported on turso-server", def.Defname))
			}
		}
	}
	if cp.Query != nil {
		return cp.Query.SqlString(), nil
	}
	if cp.Relation == nil {
		return "", mterrors.NewPgError("ERROR", "42601", "COPY has no table or query", "")
	}
	cols := "*"
	if cp.Attlist != nil && len(cp.Attlist.Items) > 0 {
		names := make([]string, 0, len(cp.Attlist.Items))
		for _, item := range cp.Attlist.Items {
			str, ok := item.(*ast.String)
			if !ok {
				return "", mterrors.NewPgError("ERROR", "42601", "unexpected COPY column list", "")
			}
			names = append(names, ast.QuoteIdentifier(str.SVal))
		}
		cols = strings.Join(names, ", ")
	}
	return "SELECT " + cols + " FROM " + cp.Relation.SqlString(), nil
}

func defString(def *ast.DefElem) string {
	if str, ok := def.Arg.(*ast.String); ok {
		return str.SVal
	}
	if def.Arg != nil {
		return def.Arg.SqlString()
	}
	return ""
}

// appendCopyRow appends row in COPY text format: tab-separated, \N for
// NULL, backslash escapes, newline-terminated.
func appendCopyRow(buf []byte, row *sqltypes.Row) []byte {
	for i, v := range row.Values {
		if i > 0 {
			buf = append(buf, '\t')
		}
		if v == nil {
			buf = append(buf, '\\', 'N')
			continue
		}
		for _, c := range []byte(v) {
			switch c {
			case '\\':
				buf = append(buf, '\\', '\\')
			case '\b':
				buf = append(buf, '\\', 'b')
			case '\f':
				buf = append(buf, '\\', 'f')
			case '\n':
				buf = append(buf, '\\', 'n')
			case '\r':
				buf = append(buf, '\\', 'r')
			case '\t':
				buf = append(buf, '\\', 't')
			case '\v':
				buf = append(buf, '\\', 'v')
			default:
				buf = append(buf, c)
			}
		}
	}
	return append(buf, '\n')
}
