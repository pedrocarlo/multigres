# Hranagres

Hranagres serves one turso-server database over the PostgreSQL wire
protocol. It is built as the `hranagres` binary (`go/cmd/hranagres`) and runs
two parts in one process:

- **The multigateway**, unchanged: it accepts PostgreSQL clients (`psql`,
  `pg_dump`, drivers), authenticates them with SCRAM-SHA-256, parses and plans
  every statement, and keeps all session state (settings, prepared statements,
  portals, the transaction state).
- **The Hrana pooler** (`go/services/hranapooler`) in place of the multipooler:
  it serves the multipooler query service (`MultipoolerService`) on the
  in-memory gRPC server, as Minigres's pooler does, and sends each statement to
  turso-server as Hrana over HTTP (`POST /v3/pipeline`, `go/common/hrana`).

The PostgreSQL protocol therefore ends at the gateway. turso-server only sees
Hrana, and stays stateless: a statement outside a transaction is one pipeline
that opens and closes its own stream. The only state on turso-server is the
Hrana stream (baton) of a transaction.

It needs no etcd (the gateway registers itself in an in-memory topology) and no
PostgreSQL.

## Running it

```bash
make build
export HRANAGRES_PASSWORD=secret
bin/hranagres --pg-port 15432 --http-port 15001 --grpc-port 15991 \
  --hrana-url http://127.0.0.1:8080 \
  --hrana-host '<db>--<tenant>--<group>.turso'

PGPASSWORD=secret psql "host=127.0.0.1 port=15432 user=postgres dbname=postgres"
PGPASSWORD=secret pg_dump "host=127.0.0.1 port=15432 user=postgres dbname=postgres" -f dump.sql
```

Like every Multigres service it refuses to run as root.

| Flag                         | Env                          | Meaning                                                                                                        |
| ---------------------------- | ---------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `--hrana-url`                | `HRANAGRES_HRANA_URL`        | turso-server's user API, or db-router                                                                          |
| `--hrana-host`               | `HRANAGRES_HRANA_HOST`       | `Host` header that selects the database: `{db}--{tenant}--{group}.turso`. Required                             |
| `--hrana-token-file`         | `HRANAGRES_HRANA_TOKEN_FILE` | Bearer token for turso-server, read again every minute                                                         |
| `--hrana-timeout`            |                              | Timeout of one Hrana request (default 10m: a `COPY` of a large table is one request)                           |
| `--database`                 | `HRANAGRES_DATABASE`         | Database name clients connect with (default `postgres`)                                                        |
| `--password-file`            | `HRANAGRES_PASSWORD`         | Password every user logs in with. Required: turso-server has no `pg_authid`                                    |
| `--server-version` (gateway) | `MT_SERVER_VERSION`          | `server_version` reported to clients. `pg_dump` refuses a server newer than itself and picks its queries by it |

The gateway's own flags (`--pg-port`, `--statement-timeout`, TLS, ...) work as
in Multigres.

## How statements map to Hrana

| PostgreSQL                             | Hrana                                                                                                        |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Statement outside a transaction        | One pipeline: `execute`, `close`                                                                             |
| `BEGIN` ... `COMMIT`/`ROLLBACK`        | A reserved connection is a Hrana stream: `BEGIN` goes out with the first statement, the conclusion closes it |
| Extended protocol (Parse/Bind/Execute) | `execute` with positional arguments; `Describe` is a Hrana `describe`. Text format only                      |
| `COPY ... TO STDOUT`                   | Rewritten to the `SELECT` it reads; the rows are encoded in COPY text format by the pooler                   |
| `set_config` probe of the gateway      | Answered by the pooler: the gateway keeps session settings itself                                            |
| `SET` that turso-server refuses        | Answered as a no-op (turso-server refuses `SET` before the PostgreSQL session sees it)                       |

Column types come from Hrana's `decltype`, mapped as turso's own wire server
maps them. Booleans, which turso stores as 0/1, are sent as `t`/`f`.

## Not supported

`COPY FROM STDIN`, binary parameters and results, `LISTEN`/`NOTIFY`, logical
replication, SQL-level `EXECUTE` of a prepared statement, `COMMIT AND CHAIN`,
and portal row limits (all rows are returned). Errors carry a SQLSTATE mapped
from turso's message where it is clear, `XX000` otherwise: turso-server sends
none.

## pg_dump

`pg_dump` runs through the gateway unchanged. How far it gets depends on the
PostgreSQL emulation of the turso version turso-server is built with (its
catalog, `pg_get_*def` functions and SQL coverage), not on Hrana. With turso
`2ab8e5e3` it gets through the connection setup, the transaction, `LOCK TABLE`
and most catalog queries, and stops at the index query
(`array_agg(... ORDER BY ...)` is not supported there). turso-server also needs
a larger SQL stack than its 65 KiB default for some `pg_dump` queries
(`TURSOD_SQLITE_SUSPEND_EXECUTION_STACK_SIZE`), or the whole server aborts on a
stack overflow.
