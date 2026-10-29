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

// hranagres runs a multigateway in front of one turso-server database: the
// gateway speaks the PostgreSQL wire protocol to clients, and its pooler, in
// the same process, sends every statement to turso-server as Hrana over HTTP
// (see go/services/hranapooler). It needs no etcd and no PostgreSQL.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/common/hrana"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/hranapooler"
	"github.com/multigres/multigres/go/services/multigateway"
	"github.com/multigres/multigres/go/services/multigateway/poolergateway"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"
)

const serviceName = "hranagres"

// options are hranagres's own settings; the gateway's are its usual flags.
type options struct {
	hranaURL       string
	hranaHost      string
	hranaTokenFile string
	hranaTimeout   time.Duration
	database       string
	passwordFile   string
}

type hranagres struct {
	opts            options
	senv            *servenv.ServEnv
	grpcServer      *servenv.GrpcServer
	queryGrpcServer *servenv.GrpcServer
	gateway         *multigateway.Multigateway
	pooler          *hranapooler.Server
}

func newHranagres() *hranagres {
	reg := viperutil.NewRegistry()
	tel := telemetry.NewTelemetry()
	h := &hranagres{
		senv:       servenv.NewServEnvWithConfig(reg, servenv.NewLogger(reg, tel), viperutil.NewViperConfig(reg), tel),
		grpcServer: servenv.NewGrpcServer(reg),
	}
	h.queryGrpcServer = servenv.NewInMemoryGrpcServer(h.grpcServer)
	resources := servenv.ProcessResources{
		ServEnv:         h.senv,
		GrpcServer:      h.grpcServer,
		QueryGrpcServer: h.queryGrpcServer,
	}
	local := &poolergateway.LocalPooler{
		ID:          h.poolerID,
		DialOptions: h.queryGrpcServer.DialOptions(),
		Record:      func() *clustermetadatapb.Multipooler { return h.pooler.Record(h.senv.GetHostname()) },
	}
	h.gateway = multigateway.NewMultigateway(reg, resources, "/", local)
	return h
}

func (h *hranagres) registerFlags(fs *pflag.FlagSet) {
	h.senv.RegisterFlags(fs)
	h.grpcServer.RegisterFlags(fs)
	h.gateway.RegisterFlags(fs)
	fs.StringVar(&h.opts.hranaURL, "hrana-url", envOr("HRANAGRES_HRANA_URL", "http://127.0.0.1:8080"), "turso-server user API or db-router URL (env HRANAGRES_HRANA_URL)")
	fs.StringVar(&h.opts.hranaHost, "hrana-host", os.Getenv("HRANAGRES_HRANA_HOST"), "Host header that selects the database: {db}--{tenant}--{group}.turso (env HRANAGRES_HRANA_HOST)")
	fs.StringVar(&h.opts.hranaTokenFile, "hrana-token-file", os.Getenv("HRANAGRES_HRANA_TOKEN_FILE"), "file with the bearer token for turso-server, read again every minute (env HRANAGRES_HRANA_TOKEN_FILE)")
	fs.DurationVar(&h.opts.hranaTimeout, "hrana-timeout", 10*time.Minute, "timeout of one Hrana request")
	fs.StringVar(&h.opts.database, "database", envOr("HRANAGRES_DATABASE", "postgres"), "database name clients connect with (env HRANAGRES_DATABASE)")
	fs.StringVar(&h.opts.passwordFile, "password-file", os.Getenv("HRANAGRES_PASSWORD_FILE"), "file with the password every user logs in with (SCRAM-SHA-256); env HRANAGRES_PASSWORD sets it directly")
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func (h *hranagres) preRun(cmd *cobra.Command) error {
	fs := cmd.Flags()
	if !fs.Changed("service-id") {
		if err := fs.Set("service-id", servenv.GenerateRandomServiceID()); err != nil {
			return err
		}
	}
	if err := h.gateway.CobraPreRunE(cmd); err != nil {
		return err
	}
	return h.senv.CobraPreRunE(cmd)
}

func (h *hranagres) password() (string, error) {
	if h.opts.passwordFile != "" {
		b, err := os.ReadFile(h.opts.passwordFile)
		if err != nil {
			return "", fmt.Errorf("read password file: %w", err)
		}
		return strings.TrimRight(string(b), "\r\n"), nil
	}
	return os.Getenv("HRANAGRES_PASSWORD"), nil
}

func (h *hranagres) run(ctx context.Context) error {
	if h.opts.hranaHost == "" {
		return errors.New("--hrana-host is required: it selects the turso-server database")
	}
	password, err := h.password()
	if err != nil {
		return err
	}
	if password == "" {
		return errors.New("a password is required (--password-file or HRANAGRES_PASSWORD)")
	}

	id := h.gateway.ServiceIdentity()
	id.ServiceName = serviceName
	id.Database = h.opts.database
	if err := h.senv.Init(id); err != nil {
		return fmt.Errorf("servenv init: %w", err)
	}

	client := hrana.NewClient(hrana.Config{
		URL:        h.opts.hranaURL,
		Host:       h.opts.hranaHost,
		Token:      tokenReader(h.opts.hranaTokenFile),
		HTTPClient: &http.Client{Timeout: h.opts.hranaTimeout},
	})
	h.pooler, err = hranapooler.New(hranapooler.Config{
		ID:       h.poolerID(),
		Database: h.opts.database,
		Client:   client,
		Password: password,
		Logger:   h.senv.GetLogger(),
	})
	if err != nil {
		return err
	}
	h.senv.OnRun(func() {
		multipoolerpb.RegisterMultipoolerServiceServer(h.queryGrpcServer.Server, h.pooler)
	})

	// The gateway registers itself in a topology; nothing reads it here, so it
	// is kept in memory.
	ts := memorytopo.NewServer(ctx, id.Cell)
	defer ts.Close()
	if err := h.gateway.Init(ctx, ts); err != nil {
		return err
	}
	slog.InfoContext(ctx, "hranagres serving turso-server database", "url", h.opts.hranaURL, "host", h.opts.hranaHost, "database", h.opts.database)
	return h.senv.RunDefault(h.grpcServer, h.queryGrpcServer)
}

func (h *hranagres) poolerID() *clustermetadatapb.ID {
	id := h.gateway.ServiceIdentity()
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      id.Cell,
		Name:      id.ServiceInstanceID,
	}
}

// tokenReader returns the bearer token from path, read again at most once a
// minute so a rotated token is picked up. No path means no token.
func tokenReader(path string) func() (string, error) {
	if path == "" {
		return nil
	}
	var (
		mu    sync.Mutex
		token string
		read  time.Time
	)
	return func() (string, error) {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(read) < time.Minute && token != "" {
			return token, nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		token, read = strings.TrimSpace(string(b)), time.Now()
		return token, nil
	}
}

// CreateHranagresCommand returns the hranagres command.
func CreateHranagresCommand() *cobra.Command {
	h := newHranagres()
	cmd := &cobra.Command{
		Use:   serviceName,
		Short: "hranagres serves one turso-server database over the PostgreSQL wire protocol, through a multigateway that talks Hrana to turso-server.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return h.preRun(cmd)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return h.run(cmd.Context())
		},
	}
	h.registerFlags(cmd.Flags())
	return cmd
}

func main() {
	if err := CreateHranagresCommand().Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1) //nolint:forbidigo // main() is allowed to call os.Exit
	}
}
