"""SQL store for transfer history.

The store folds the started, progress and completed messages of a transfer into one
row of the transfers table. Ingest is idempotent: the same message delivered twice
(normal with pub/sub and retries) is recognised by its fingerprint and ignored.
Messages may arrive out of order; a completed message that arrives before its
started message still produces a correct row.
"""

from __future__ import annotations

import hashlib
import ntpath
import posixpath
import sqlite3
from dataclasses import dataclass
from importlib import resources
from typing import Iterable, Optional, Sequence

from .model import TransferEvent, status_for
from .timeutil import ago, fmt_ts, normalize, now

ACTION_RANK = {"started": 1, "progress": 2, "completed": 3, "cancelled": 3}


def _basename(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    return ntpath.basename(posixpath.basename(path)) or path


def _fingerprint(ev: TransferEvent) -> str:
    h = hashlib.sha256()
    for part in (ev.transfer_id, ev.action, ev.event_time or "", str(ev.result_code),
                 str(ev.bytes_sent), str(len(ev.items))):
        h.update(part.encode())
        h.update(b"\x1f")
    for it in ev.items:
        h.update(f"{it.source_file}|{it.destination_file}|{it.result_code}".encode())
    return h.hexdigest()


@dataclass
class Scope:
    """What a caller may see. agents=None means every agent (admin)."""

    name: str
    agents: Optional[frozenset[str]] = None

    def allows(self, row: dict) -> bool:
        if self.agents is None:
            return True
        return row.get("source_agent") in self.agents or row.get("destination_agent") in self.agents


ADMIN = Scope("admin", None)


class HistoryStore:
    def __init__(self, path: str = "mfthistory.db"):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        if path != ":memory:":
            self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.executescript(resources.files("mfthistory").joinpath("schema.sql").read_text())

    def close(self) -> None:
        self.conn.close()

    # ---------------------------------------------------------------- ingest
    def ingest(self, ev: TransferEvent, commit: bool = True) -> bool:
        """Store one event. Returns False when the event was a duplicate.

        Pass commit=False to group many events in one database transaction, then call
        commit(). Collectors do this for speed and acknowledge their source only after.
        """
        received = fmt_ts(now())
        event_time = normalize(ev.event_time) or received
        fp = _fingerprint(ev)
        cur = self.conn.cursor()
        try:
            cur.execute(
                "INSERT INTO transfer_events (transfer_id, action, event_time, received_at, fingerprint, raw_xml)"
                " VALUES (?,?,?,?,?,?)",
                (ev.transfer_id, ev.action, event_time, received, fp, ev.raw_xml),
            )
        except sqlite3.IntegrityError:
            return False

        row = cur.execute("SELECT * FROM transfers WHERE transfer_id = ?", (ev.transfer_id,)).fetchone()
        rank = ACTION_RANK.get(ev.action, 0)
        if row is None:
            cur.execute(
                "INSERT INTO transfers (transfer_id, updated_at) VALUES (?, ?)", (ev.transfer_id, received)
            )
            row = cur.execute("SELECT * FROM transfers WHERE transfer_id = ?", (ev.transfer_id,)).fetchone()
        current_rank = ACTION_RANK.get(row["last_action"] or "", 0)
        newer = rank > current_rank or (
            rank == current_rank and (row["last_event_at"] or "") <= event_time
        )

        fields = {
            "source_agent": row["source_agent"] or ev.source_agent,
            "source_qmgr": row["source_qmgr"] or ev.source_qmgr,
            "destination_agent": row["destination_agent"] or ev.destination_agent,
            "destination_qmgr": row["destination_qmgr"] or ev.destination_qmgr,
            "originator_host": row["originator_host"] or ev.originator_host,
            "originator_user": row["originator_user"] or ev.originator_user,
            "job_name": row["job_name"] or ev.job_name,
            "updated_at": received,
        }
        if ev.action == "started":
            fields["started_at"] = min(filter(None, [row["started_at"], event_time]))
        elif not row["started_at"]:
            # Started message lost or not yet seen: use the earliest time we know.
            fields["started_at"] = event_time
        if newer:
            fields["last_action"] = ev.action
            fields["last_event_at"] = event_time
            if ev.action in ("completed", "cancelled"):
                fields["completed_at"] = event_time
                fields["result_code"] = ev.result_code
                fields["supplement"] = ev.supplement
            fields["status"] = status_for(
                ev.result_code if ev.action in ("completed", "cancelled") else row["result_code"],
                ev.action,
            )
        if ev.bytes_sent is not None and newer:
            fields["total_bytes"] = ev.bytes_sent

        # Items: the completed message carries the final per file result, so it wins.
        have_items = cur.execute(
            "SELECT COUNT(*) FROM transfer_items WHERE transfer_id = ?", (ev.transfer_id,)
        ).fetchone()[0]
        if ev.items and (have_items == 0 or ev.action in ("completed", "cancelled")):
            cur.execute("DELETE FROM transfer_items WHERE transfer_id = ?", (ev.transfer_id,))
            cur.executemany(
                "INSERT INTO transfer_items (transfer_id, seq, source_file, source_name, destination_file,"
                " source_size, destination_size, mode, checksum_method, checksum, result_code, supplement)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (ev.transfer_id, i, it.source_file, _basename(it.source_file), it.destination_file,
                     it.source_size, it.destination_size, it.mode, it.checksum_method, it.checksum,
                     it.result_code, it.supplement)
                    for i, it in enumerate(ev.items)
                ],
            )
            fields["file_count"] = len(ev.items)
            if ev.bytes_sent is None:
                sizes = [it.source_size for it in ev.items if it.source_size is not None]
                if sizes:
                    fields["total_bytes"] = sum(sizes)

        sets = ", ".join(f"{k} = ?" for k in fields)
        cur.execute(f"UPDATE transfers SET {sets} WHERE transfer_id = ?", (*fields.values(), ev.transfer_id))
        if commit:
            self.conn.commit()
        return True

    def commit(self) -> None:
        self.conn.commit()

    def ingest_many(self, events: Iterable[TransferEvent]) -> tuple[int, int]:
        stored = dupes = 0
        for ev in events:
            if self.ingest(ev, commit=False):
                stored += 1
            else:
                dupes += 1
        self.conn.commit()
        return stored, dupes

    # ---------------------------------------------------------------- queries
    @staticmethod
    def _scope_sql(scope: Scope, alias: str = "t") -> tuple[str, list]:
        if scope.agents is None:
            return "", []
        if not scope.agents:
            return " AND 1 = 0", []
        marks = ",".join("?" * len(scope.agents))
        agents = sorted(scope.agents)
        return (f" AND ({alias}.source_agent IN ({marks}) OR {alias}.destination_agent IN ({marks}))",
                agents + agents)

    def search(
        self,
        scope: Scope = ADMIN,
        transfer_id: Optional[str] = None,
        agent: Optional[str] = None,
        file: Optional[str] = None,
        status: Optional[str] = None,
        job: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict:
        where = ["1 = 1"]
        args: list = []
        if transfer_id:
            where.append("t.transfer_id = ?")
            args.append(transfer_id)
        if agent:
            where.append("(t.source_agent = ? OR t.destination_agent = ?)")
            args += [agent, agent]
        if status:
            where.append("t.status = ?")
            args.append(status)
        if job:
            where.append("t.job_name LIKE ?")
            args.append(f"%{job}%")
        if since:
            where.append("t.started_at >= ?")
            args.append(normalize(since) or since)
        if until:
            where.append("t.started_at < ?")
            args.append(normalize(until) or until)
        if file:
            pattern = file.replace("*", "%")
            if "%" not in pattern:
                pattern = f"%{pattern}%"
            where.append(
                "EXISTS (SELECT 1 FROM transfer_items i WHERE i.transfer_id = t.transfer_id"
                " AND (i.source_name LIKE ? OR i.source_file LIKE ? OR i.destination_file LIKE ?))"
            )
            args += [pattern, pattern, pattern]
        scope_sql, scope_args = self._scope_sql(scope)
        sql_where = " AND ".join(where) + scope_sql
        args += scope_args
        limit = max(1, min(int(limit), 500))
        total = self.conn.execute(f"SELECT COUNT(*) FROM transfers t WHERE {sql_where}", args).fetchone()[0]
        rows = self.conn.execute(
            f"SELECT t.* FROM transfers t WHERE {sql_where}"
            " ORDER BY t.started_at DESC, t.transfer_id LIMIT ? OFFSET ?",
            [*args, limit, int(offset)],
        ).fetchall()
        return {"total": total, "limit": limit, "offset": int(offset), "results": [dict(r) for r in rows]}

    def get(self, transfer_id: str, scope: Scope = ADMIN) -> Optional[dict]:
        row = self.conn.execute("SELECT * FROM transfers WHERE transfer_id = ?", (transfer_id,)).fetchone()
        if row is None or not scope.allows(dict(row)):
            return None
        out = dict(row)
        out["items"] = [dict(r) for r in self.conn.execute(
            "SELECT seq, source_file, destination_file, source_size, destination_size, mode,"
            " checksum_method, checksum, result_code, supplement FROM transfer_items"
            " WHERE transfer_id = ? ORDER BY seq", (transfer_id,))]
        out["events"] = [dict(r) for r in self.conn.execute(
            "SELECT action, event_time, received_at FROM transfer_events WHERE transfer_id = ?"
            " ORDER BY event_time, id", (transfer_id,))]
        return out

    def stuck(self, older_than_minutes: float = 60, scope: Scope = ADMIN, ref=None) -> list[dict]:
        """Transfers that started but never completed. These are the ones that need a person."""
        scope_sql, scope_args = self._scope_sql(scope)
        rows = self.conn.execute(
            "SELECT t.* FROM transfers t WHERE t.status = 'in_progress' AND t.last_event_at < ?"
            f"{scope_sql} ORDER BY t.started_at",
            [ago(minutes=older_than_minutes, ref=ref), *scope_args],
        ).fetchall()
        return [dict(r) for r in rows]

    def stats(self, scope: Scope = ADMIN, since: Optional[str] = None) -> dict:
        scope_sql, scope_args = self._scope_sql(scope)
        since_sql, since_args = ("", [])
        if since:
            since_sql, since_args = " AND t.started_at >= ?", [normalize(since) or since]
        base = f"FROM transfers t WHERE 1 = 1{since_sql}{scope_sql}"
        args = [*since_args, *scope_args]
        by_status = {r[0] or "unknown": r[1] for r in self.conn.execute(
            f"SELECT t.status, COUNT(*) {base} GROUP BY t.status", args)}
        by_day = [dict(r) for r in self.conn.execute(
            f"SELECT substr(t.started_at, 1, 10) AS day, COUNT(*) AS transfers,"
            f" SUM(CASE WHEN t.status = 'success' THEN 1 ELSE 0 END) AS success,"
            f" SUM(CASE WHEN t.status IN ('failed', 'partial', 'cancelled') THEN 1 ELSE 0 END) AS problems,"
            f" COALESCE(SUM(t.total_bytes), 0) AS bytes {base} GROUP BY day ORDER BY day", args)]
        by_agent = [dict(r) for r in self.conn.execute(
            f"SELECT t.source_agent AS agent, COUNT(*) AS transfers,"
            f" SUM(CASE WHEN t.status = 'success' THEN 0 ELSE 1 END) AS not_success"
            f" {base} GROUP BY t.source_agent ORDER BY transfers DESC LIMIT 50", args)]
        return {"by_status": by_status, "by_day": by_day, "by_source_agent": by_agent}

    def agents(self, scope: Scope = ADMIN) -> list[str]:
        scope_sql, scope_args = self._scope_sql(scope)
        rows = self.conn.execute(
            f"SELECT source_agent AS a FROM transfers t WHERE source_agent IS NOT NULL{scope_sql}"
            f" UNION SELECT destination_agent FROM transfers t WHERE destination_agent IS NOT NULL{scope_sql}"
            " ORDER BY 1", [*scope_args, *scope_args]).fetchall()
        return [r[0] for r in rows]

    def purge(self, older_than_days: float) -> int:
        """Delete transfers (and their items and events) that started before the cutoff."""
        cutoff = ago(days=older_than_days)
        ids = [r[0] for r in self.conn.execute(
            "SELECT transfer_id FROM transfers WHERE started_at < ?", (cutoff,))]
        for chunk in _chunks(ids, 500):
            marks = ",".join("?" * len(chunk))
            self.conn.execute(f"DELETE FROM transfer_events WHERE transfer_id IN ({marks})", chunk)
            self.conn.execute(f"DELETE FROM transfers WHERE transfer_id IN ({marks})", chunk)
        self.conn.commit()
        return len(ids)


def _chunks(seq: Sequence, n: int):
    for i in range(0, len(seq), n):
        yield list(seq[i:i + n])
