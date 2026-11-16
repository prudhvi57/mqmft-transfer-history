# Architecture

## Components

| Module | Job |
|---|---|
| `parser.py` | Turns one transfer log XML message into a `TransferEvent`. Strips namespaces, tolerates missing optional elements, rejects anything that is not a `<transaction>` with an ID. |
| `store.py` | SQL store. Folds events into transfers, keeps per file results and every message, runs searches with access scope, stuck detection, stats and retention. |
| `collectors.py` | `MQCollector` (durable subscription on `SYSTEM.FTE`, commit after store) and `DirectoryCollector` (folder of XML files, moved only after their batch commits). |
| `auth.py` | Token registry. Stores SHA-256 hashes only. Maps each token to all agents or a fixed list. |
| `server.py` | Standard library HTTP server for the JSON API, CSV export and the web page. |
| `simulate.py` | Synthetic message generator for demos and tests. |
| `cli.py` | `mfthistory` command. |

## Why fold instead of just storing messages

A business user wants one answer per transfer: did it work, which files, when. MQ MFT
emits several messages per transfer, and pub/sub delivery can repeat or reorder them.
Folding gives one row per transfer that is always the best current answer, while the
`transfer_events` table keeps every message for audit.

### Folding rules

1. **Fingerprint first.** Each message gets a SHA-256 fingerprint over transfer ID,
   action, time, result code, bytes and file list. A repeat is dropped before it touches
   the transfer row. This makes ingest idempotent, so replaying a folder or a
   subscription backlog is safe.
2. **Rank actions.** started (1) < progress (2) < completed or cancelled (3). A message
   only updates status if its rank is higher, or equal rank with a later time.
3. **Earliest start wins.** If the started message is lost, the earliest event time we
   saw becomes the start time.
4. **Final file results win.** Per file results come from the completed message. A late
   started message cannot wipe them.
5. **Status is derived** from result code and last action (see the README table).

## Access scope

Every query takes a `Scope`. Admin scopes see everything. A partner scope has a set of
agent names and every query adds
`AND (source_agent IN (...) OR destination_agent IN (...))`. A transfer outside the
scope returns 404, not 403, so the API does not confirm that a transfer ID exists.

## Data model

See `src/mfthistory/schema.sql`. Times are stored as UTC ISO 8601 text with
milliseconds so they sort as strings on every database. Indexes cover the common
searches: by start time, by agent and time, by status and time, by job, and by file name.

## Reliability choices

- Durable, managed subscription: messages published while the collector is down wait on
  MQ and are delivered on restart.
- Syncpoint get plus commit after the database insert: a crash between the two means the
  message is delivered again, and the fingerprint drops the repeat. At least once in,
  exactly once stored.
- Folder collector moves files only after their batch commits, with the same effect.
