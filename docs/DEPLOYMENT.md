# Deployment notes

## 1. MQ side

The collector connects as a client to the **coordination queue manager** and subscribes to
`SYSTEM.FTE` with topic string `Log/#`.

Suggested objects (adjust names to your standards):

```
DEFINE CHANNEL(MFT.HISTORY.SVRCONN) CHLTYPE(SVRCONN) TRPTYPE(TCP) SSLCIPH(...) REPLACE
SET CHLAUTH(MFT.HISTORY.SVRCONN) TYPE(ADDRESSMAP) ADDRESS('<collector host>') USERSRC(CHANNEL) CHCKCLNT(REQUIRED)
SET AUTHREC PROFILE('SYSTEM.FTE') OBJTYPE(TOPIC) PRINCIPAL('mfthist') AUTHADD(SUB)
SET AUTHREC OBJTYPE(QMGR) PRINCIPAL('mfthist') AUTHADD(CONNECT, INQ)
```

The subscription is durable and managed, named `MFTHISTORY.SUB` by default. While the
collector is stopped, messages queue up for it, so watch the managed queue depth and
set its MAXDEPTH for your longest planned outage. Use `Log/<AGENT>/#` to follow a
single agent.

Use TLS on the channel. Read the MQ password from `MFTHISTORY_MQ_PASSWORD`, never from
the command line.

## 2. Sizing and retention

Rough rule: two to three messages per transfer (started, completed, and progress for
large or multi file transfers). For a platform moving 500,000 files a day, plan for
1 to 1.5 million messages a day, mostly small. With per file rows and no raw XML, each
transfer uses well under 1 KB in SQLite.

Pick a retention that matches your audit rules and run it daily:

```
mfthistory purge --days 400
```

`--keep-raw` stores the original XML for every message. It helps audits but grows the
database several times faster.

## 3. Moving to SQL Server, Db2 or PostgreSQL

`schema.sql` is plain SQL. Change `TEXT` to `VARCHAR(...)` and `INTEGER PRIMARY KEY` to an
identity column. The store uses only standard SQL (no SQLite specific functions except
`substr`, which all of these support). Swap the connection in `HistoryStore.__init__` for
your DB API driver (pyodbc, ibm_db, psycopg) and change the `?` placeholders if the driver
needs another style.

## 4. Web access

`mfthistory serve` binds to localhost by default. For partner access put it behind your
reverse proxy with TLS and, if you have it, SSO. Tokens go in the `Authorization: Bearer`
header. Issue one token per partner or team, list them with `mfthistory token list` and
revoke with `mfthistory token revoke --name ...`.

## 5. Running as services

Run two long lived processes, for example as systemd units:

```
mfthistory --db /var/lib/mfthistory/h.db collect-mq --qmgr COORDQM --channel MFT.HISTORY.SVRCONN --conn "mqhost(1414)" --user mfthist
mfthistory --db /var/lib/mfthistory/h.db serve --host 127.0.0.1 --port 8080 --tokens /etc/mfthistory/tokens.json
```
