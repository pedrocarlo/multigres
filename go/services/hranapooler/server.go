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

// Package hranapooler serves the multipooler query service
// (MultipoolerService) over a turso-server database instead of PostgreSQL.
// Each statement goes to turso-server as Hrana over HTTP (`/v3/pipeline`).
//
// The multigateway in front of it keeps everything stateful about a client
// connection (the session, prepared statements, the wire protocol); the only
// server-side state is a Hrana stream (baton) per reserved connection, which
// is how a transaction lives on turso-server. Everything else is one
// autocommit pipeline per statement, so turso-server stays stateless.
package hranapooler

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/hrana"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/scram"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryrpc"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// healthInterval is how often the health stream repeats the (static) state.
const healthInterval = 5 * time.Second

// Config configures a Server.
type Config struct {
	// ID is the pooler's topology ID; the gateway's local pooler.
	ID *clustermetadatapb.ID
	// Database is the database name clients connect to. The gateway routes a
	// connection to the pooler whose record has the same database.
	Database string
	// Client sends Hrana pipelines to the turso-server database.
	Client *hrana.Client
	// Password, when set, is the password of every user: the gateway checks
	// it with SCRAM-SHA-256. turso-server has no pg_authid to read users from.
	Password string
	// Logger logs; slog.Default() when nil.
	Logger *slog.Logger
}

// Server implements MultipoolerService over Hrana.
type Server struct {
	multipoolerpb.UnimplementedMultipoolerServiceServer

	cfg        Config
	logger     *slog.Logger
	scramHash  string
	mu         sync.Mutex
	nextConnID uint64
	reserved   map[uint64]*reservedConn
}

// reservedConn is a pinned "connection": one Hrana stream.
type reservedConn struct {
	id      uint64
	stream  *hrana.Stream
	reasons uint32
}

// New returns a server for cfg.
func New(cfg Config) (*Server, error) {
	if cfg.ID == nil || cfg.Client == nil {
		return nil, errors.New("hranapooler: ID and Client are required")
	}
	if cfg.Database == "" {
		cfg.Database = constants.DefaultPostgresDatabase
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{cfg: cfg, logger: logger, reserved: make(map[uint64]*reservedConn)}
	if cfg.Password != "" {
		hash, err := scramVerifier(cfg.Password)
		if err != nil {
			return nil, err
		}
		s.scramHash = hash
	}
	return s, nil
}

// Record is the pooler's topology record. The gateway learns of its local
// pooler from it.
func (s *Server) Record(hostname string) *clustermetadatapb.Multipooler {
	return &clustermetadatapb.Multipooler{
		Id: s.cfg.ID,
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   s.cfg.Database,
			TableGroup: constants.DefaultTableGroup,
			Shard:      constants.DefaultShard,
		},
		ServingStatus: clustermetadatapb.PoolerServingStatus_SERVING,
		Hostname:      hostname,
		PortMap:       map[string]int32{},
	}
}

// scramVerifier returns a SCRAM-SHA-256 verifier for password, in pg_authid
// format.
func scramVerifier(password string) (string, error) {
	const iterations = 4096
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("hranapooler: salt: %w", err)
	}
	salted := scram.ComputeSaltedPassword(password, salt, iterations)
	stored := scram.ComputeStoredKey(scram.ComputeClientKey(salted))
	server := scram.ComputeServerKey(salted)
	b64 := base64.StdEncoding.EncodeToString
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", iterations, b64(salt), b64(stored), b64(server)), nil
}

// GetAuthCredentials returns the verifier of the configured password for
// every user. Without a password no user can log in.
func (s *Server) GetAuthCredentials(_ context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
	if req.GetUsername() == "" {
		return nil, status.Error(codes.InvalidArgument, "username is required")
	}
	if s.scramHash == "" {
		return nil, status.Errorf(codes.NotFound, "role %q does not exist", req.GetUsername())
	}
	return &multipoolerpb.GetAuthCredentialsResponse{ScramHash: s.scramHash}, nil
}

// StreamPoolerHealth reports a serving, writable pooler: turso-server has no
// replicas and no failover the gateway could act on.
func (s *Server) StreamPoolerHealth(_ *multipoolerpb.StreamPoolerHealthRequest, stream multipoolerpb.MultipoolerService_StreamPoolerHealthServer) error {
	resp := &multipoolerpb.StreamPoolerHealthResponse{
		PoolerId:                    s.cfg.ID,
		ServingStatus:               clustermetadatapb.PoolerServingStatus_SERVING,
		RoutingState:                &clustermetadatapb.RoutingState{Role: clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY},
		RecommendedStalenessTimeout: durationpb.New(6 * healthInterval),
	}
	ticker := time.NewTicker(healthInterval)
	defer ticker.Stop()
	for {
		if err := stream.Send(resp); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
		}
	}
}

// ExecuteStream serves the gateway's reusable query stream.
func (s *Server) ExecuteStream(stream multipoolerpb.MultipoolerService_ExecuteStreamServer) error {
	return queryrpc.Serve(stream, s)
}

// newReserved pins a new Hrana stream.
func (s *Server) newReserved(reasons uint32) *reservedConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextConnID++
	rc := &reservedConn{id: s.nextConnID, stream: s.cfg.Client.NewStream(), reasons: reasons}
	s.reserved[rc.id] = rc
	return rc
}

func (s *Server) getReserved(id uint64) (*reservedConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rc, ok := s.reserved[id]
	if !ok {
		return nil, mterrors.NewReservedConnectionTerminated(id)
	}
	return rc, nil
}

// release drops a reserved connection and closes its stream; an open
// transaction rolls back.
func (s *Server) release(ctx context.Context, rc *reservedConn) {
	s.mu.Lock()
	delete(s.reserved, rc.id)
	s.mu.Unlock()
	if err := rc.stream.Close(ctx); err != nil {
		s.logger.WarnContext(ctx, "hranapooler: close stream", "reserved_connection_id", rc.id, "error", err)
	}
}

func (s *Server) state(rc *reservedConn) *query.ReservedState {
	if rc == nil || rc.reasons == 0 {
		return &query.ReservedState{}
	}
	return &query.ReservedState{ReservedConnectionId: rc.id, PoolerId: s.cfg.ID, ReservationReasons: rc.reasons}
}

// connFor returns the reserved connection a request runs on, creating it
// when the request reserves one; nil means a single autocommit statement.
// A new transaction starts with BEGIN on its stream.
func (s *Server) connFor(ctx context.Context, opts *query.ExecuteOptions, ro *query.ReservationOptions) (rc *reservedConn, begin bool, err error) {
	reasons := ro.GetReasons()
	if reasons != 0 {
		if err := protoutil.ValidateReasons(reasons); err != nil {
			return nil, false, status.Errorf(codes.InvalidArgument, "invalid reservation reasons: %v", err)
		}
	}
	if id := opts.GetReservedConnectionId(); id != 0 {
		rc, err := s.getReserved(id)
		if err != nil {
			return nil, false, err
		}
		begin = reasons&protoutil.ReasonTransaction != 0 && rc.reasons&protoutil.ReasonTransaction == 0
		rc.reasons |= reasons
		return rc, begin, nil
	}
	if reasons == 0 {
		return nil, false, nil
	}
	return s.newReserved(reasons), reasons&protoutil.ReasonTransaction != 0, nil
}

// beginQuery is the statement that opens a transaction. turso-server runs
// every transaction as snapshot isolation (REPEATABLE READ) and rejects
// other levels and access modes on BEGIN, so the gateway's BEGIN options are
// not forwarded: a plain BEGIN gives the same isolation.
const beginQuery = "BEGIN"

// run executes one statement, on rc's stream or as a single autocommit
// pipeline.
func (s *Server) run(ctx context.Context, rc *reservedConn, begin bool, stmt hrana.Stmt) (*sqltypes.Result, error) {
	if ignored, ok := ignoredStatement(stmt.SQL); ok {
		return ignored, nil
	}
	s.logger.DebugContext(ctx, "hranapooler: execute", "sql", stmt.SQL, "args", len(stmt.Args), "reserved", rc != nil, "begin", begin)
	var res *hrana.StmtResult
	var err error
	if rc == nil {
		res, err = s.cfg.Client.Execute(ctx, stmt)
	} else {
		requests := []hrana.StreamRequest{hrana.ExecuteRequest(stmt)}
		if begin {
			requests = append([]hrana.StreamRequest{hrana.ExecuteRequest(hrana.Stmt{SQL: beginQuery})}, requests...)
		}
		var results []hrana.StreamResult
		results, err = rc.stream.Pipeline(ctx, requests...)
		if err == nil {
			if begin {
				if _, berr := hrana.DecodeExecute(results[0]); berr != nil {
					return nil, pgError(berr)
				}
				results = results[1:]
			}
			res, err = hrana.DecodeExecute(results[0])
		}
	}
	if err != nil {
		if r, ok := toleratedError(stmt.SQL, err); ok {
			s.logger.DebugContext(ctx, "hranapooler: statement not supported by turso-server, ignored", "error", err)
			return r, nil
		}
		return nil, pgError(err)
	}
	return resultOf(stmt.SQL, res), nil
}

// ignoredStatement answers statements that turso-server has no equivalent
// for and that change nothing a client can observe through it.
func ignoredStatement(sql string) (*sqltypes.Result, bool) {
	words := leadingWords(sql, 2)
	if len(words) == 0 {
		return &sqltypes.Result{}, true
	}
	if words[0] == "SELECT" {
		return setConfigResult(sql)
	}
	return nil, false
}

// toleratedError reports whether err for sql can be answered as success.
// turso-server refuses session statements (SET, RESET) before they reach the
// PostgreSQL session (spark-platform GAPS.md TS-7); the gateway tracks
// session settings itself, so a refused SET of a parameter turso does not
// have is answered as PostgreSQL would for a no-op.
func toleratedError(sql string, err error) (*sqltypes.Result, bool) {
	var herr *hrana.Error
	if !errors.As(err, &herr) {
		return nil, false
	}
	words := leadingWords(sql, 1)
	if len(words) == 0 || (words[0] != "SET" && words[0] != "RESET") {
		return nil, false
	}
	if !strings.Contains(herr.Message, "not supported") {
		return nil, false
	}
	return &sqltypes.Result{CommandTag: words[0]}, true
}

// StreamExecute runs one statement.
func (s *Server) StreamExecute(req *multipoolerpb.StreamExecuteRequest, stream multipoolerpb.MultipoolerService_StreamExecuteServer) error {
	ctx := stream.Context()
	if req.GetOptions().GetExecuteSqlPreparedStatement() != nil {
		return mterrors.ToGRPC(notSupported("SQL-level EXECUTE of a prepared statement is not supported on turso-server"))
	}
	rc, begin, err := s.connFor(ctx, req.GetOptions(), req.GetReservationOptions())
	if err != nil {
		return mterrors.ToGRPC(err)
	}
	res, err := s.run(ctx, rc, begin, hrana.Stmt{SQL: req.GetQuery(), WantRows: true})
	if err != nil {
		_ = stream.Send(&multipoolerpb.StreamExecuteResponse{ReservedState: s.state(rc)})
		return mterrors.ToGRPC(err)
	}
	return stream.Send(&multipoolerpb.StreamExecuteResponse{
		Result:        &query.QueryResultPayload{Payload: &query.QueryResultPayload_Result{Result: res.ToProto()}},
		ReservedState: s.state(rc),
	})
}

// ExecuteQuery runs one statement and returns its result.
func (s *Server) ExecuteQuery(ctx context.Context, req *multipoolerpb.ExecuteQueryRequest) (*multipoolerpb.ExecuteQueryResponse, error) {
	rc, begin, err := s.connFor(ctx, req.GetOptions(), nil)
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	res, err := s.run(ctx, rc, begin, hrana.Stmt{SQL: req.GetQuery(), WantRows: true})
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolerpb.ExecuteQueryResponse{Result: res.ToProto(), ReservedState: s.state(rc)}, nil
}

// PortalStreamExecute runs a bound prepared statement. Hrana has no
// server-side prepared statements: the query is sent with its arguments.
func (s *Server) PortalStreamExecute(req *multipoolerpb.PortalStreamExecuteRequest, stream multipoolerpb.MultipoolerService_PortalStreamExecuteServer) error {
	ctx := stream.Context()
	ps := req.GetPreparedStatement()
	args, err := bindArgs(ps.GetParamTypes(), req.GetPortal())
	if err != nil {
		return mterrors.ToGRPC(err)
	}
	if err := checkResultFormats(req.GetPortal().GetResultFormats()); err != nil {
		return mterrors.ToGRPC(err)
	}
	rc, begin, err := s.connFor(ctx, req.GetOptions(), req.GetReservationOptions())
	if err != nil {
		return mterrors.ToGRPC(err)
	}
	res, err := s.run(ctx, rc, begin, hrana.Stmt{SQL: ps.GetQuery(), Args: args, WantRows: true})
	if err != nil {
		_ = stream.Send(&multipoolerpb.PortalStreamExecuteResponse{ReservedState: s.state(rc)})
		return mterrors.ToGRPC(err)
	}
	return stream.Send(&multipoolerpb.PortalStreamExecuteResponse{
		Result:        &query.QueryResultPayload{Payload: &query.QueryResultPayload_Result{Result: res.ToProto()}},
		ReservedState: s.state(rc),
	})
}

// Describe describes a prepared statement or portal with a Hrana describe.
func (s *Server) Describe(ctx context.Context, req *multipoolerpb.DescribeRequest) (*multipoolerpb.DescribeResponse, error) {
	ps := req.GetPreparedStatement()
	if ps == nil {
		return nil, status.Error(codes.InvalidArgument, "prepared statement is required")
	}
	var results []hrana.StreamResult
	var err error
	if id := req.GetOptions().GetReservedConnectionId(); id != 0 {
		rc, rerr := s.getReserved(id)
		if rerr != nil {
			return nil, mterrors.ToGRPC(rerr)
		}
		results, err = rc.stream.Pipeline(ctx, hrana.DescribeRequest(ps.GetQuery()))
	} else {
		results, err = s.cfg.Client.NewStream().Pipeline(ctx, hrana.DescribeRequest(ps.GetQuery()), hrana.CloseRequest())
	}
	var d *hrana.DescribeResult
	if err == nil {
		d, err = hrana.DecodeDescribe(results[0])
	}
	if err != nil {
		return nil, mterrors.ToGRPC(pgError(err))
	}
	desc := &query.StatementDescription{}
	if req.GetPortal() == nil {
		desc.Parameters = make([]*query.ParameterDescription, len(d.Params))
		for i := range d.Params {
			oid := uint32(0)
			if i < len(ps.GetParamTypes()) {
				oid = ps.GetParamTypes()[i]
			}
			if oid == 0 {
				oid = uint32(textOID)
			}
			desc.Parameters[i] = &query.ParameterDescription{DataTypeOid: oid}
		}
	}
	if len(d.Cols) > 0 {
		desc.Fields, _ = fieldsOf(d.Cols)
		desc.HasFields = true
	}
	return &multipoolerpb.DescribeResponse{Description: desc}, nil
}

// ConcludeTransaction commits or rolls back the transaction of a reserved
// connection, and releases it when nothing else keeps it.
func (s *Server) ConcludeTransaction(ctx context.Context, req *multipoolerpb.ConcludeTransactionRequest) (*multipoolerpb.ConcludeTransactionResponse, error) {
	rc, err := s.getReserved(req.GetOptions().GetReservedConnectionId())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	sql := "COMMIT"
	if req.GetConclusion() == multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK {
		sql = "ROLLBACK"
	}
	if req.GetChain() {
		return nil, mterrors.ToGRPC(notSupported(sql + " AND CHAIN is not supported on turso-server"))
	}
	res, err := s.run(ctx, rc, false, hrana.Stmt{SQL: sql})
	rc.reasons &^= protoutil.ReasonTransaction | protoutil.ReasonPortal
	if rc.reasons == 0 {
		s.release(ctx, rc)
	}
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	return &multipoolerpb.ConcludeTransactionResponse{Result: res.ToProto(), ReservedState: s.state(rc)}, nil
}

// DiscardTempTables drops the temp-table reason; turso-server has no
// temporary tables tied to a session.
func (s *Server) DiscardTempTables(ctx context.Context, req *multipoolerpb.DiscardTempTablesRequest) (*multipoolerpb.DiscardTempTablesResponse, error) {
	rc, err := s.getReserved(req.GetOptions().GetReservedConnectionId())
	if err != nil {
		return nil, mterrors.ToGRPC(err)
	}
	rc.reasons &^= protoutil.ReasonTempTable
	if rc.reasons == 0 {
		s.release(ctx, rc)
	}
	return &multipoolerpb.DiscardTempTablesResponse{
		Result:        (&sqltypes.Result{CommandTag: "DISCARD TEMP"}).ToProto(),
		ReservedState: s.state(rc),
	}, nil
}

// ReleaseReservedConnection closes a reserved connection's stream; an open
// transaction rolls back.
func (s *Server) ReleaseReservedConnection(ctx context.Context, req *multipoolerpb.ReleaseReservedConnectionRequest) (*multipoolerpb.ReleaseReservedConnectionResponse, error) {
	// A connection that is already gone needs no release.
	if rc, err := s.getReserved(req.GetOptions().GetReservedConnectionId()); err == nil {
		s.release(ctx, rc)
	}
	return &multipoolerpb.ReleaseReservedConnectionResponse{ReservedState: &query.ReservedState{}}, nil
}

// notSupported is a feature_not_supported error from turso-server's side.
func notSupported(msg string) error {
	return mterrors.NewPgError("ERROR", "0A000", msg, "")
}

// NotificationStream is not supported: turso-server has no LISTEN/NOTIFY.
func (s *Server) NotificationStream(multipoolerpb.MultipoolerService_NotificationStreamServer) error {
	return mterrors.ToGRPC(notSupported("LISTEN/NOTIFY is not supported on turso-server"))
}
