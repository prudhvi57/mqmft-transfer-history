"""Collectors feed transfer log messages into the store.

DirectoryCollector  reads *.xml files from a folder (one message per file). Good for
                    tests, for replaying exported messages, and for sites that already
                    dump the SYSTEM.FTE log to disk with another tool.
MQCollector         subscribes to SYSTEM.FTE on the coordination queue manager with a
                    durable subscription, so messages published while the collector is
                    down are kept and delivered when it comes back. Needs the optional
                    pymqi package and an MQ client install.
"""

from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path
from typing import Callable, Iterator, Optional

from .parser import ParseError, parse_transfer_log
from .store import HistoryStore

log = logging.getLogger("mfthistory.collect")


class DirectoryCollector:
    def __init__(self, store: HistoryStore, folder: str, done_folder: Optional[str] = None,
                 error_folder: Optional[str] = None, keep_raw: bool = False):
        self.store = store
        self.folder = Path(folder)
        self.done = Path(done_folder) if done_folder else None
        self.errors = Path(error_folder) if error_folder else None
        self.keep_raw = keep_raw
        for p in (self.done, self.errors):
            if p:
                p.mkdir(parents=True, exist_ok=True)

    def _files(self) -> list[Path]:
        return sorted(self.folder.glob("*.xml"), key=lambda p: (p.stat().st_mtime, p.name))

    def run_once(self, batch_size: int = 500) -> dict:
        """Ingest every waiting file. Files move to done_folder only after their batch is committed."""
        counts = {"stored": 0, "duplicates": 0, "errors": 0}
        batch: list[Path] = []

        def flush():
            self.store.commit()
            if self.done:
                for p in batch:
                    shutil.move(str(p), self.done / p.name)
            batch.clear()

        for path in self._files():
            try:
                ev = parse_transfer_log(path.read_bytes(), keep_raw=self.keep_raw)
            except ParseError as exc:
                counts["errors"] += 1
                log.warning("skipping %s: %s", path.name, exc)
                if self.errors:
                    shutil.move(str(path), self.errors / path.name)
                continue
            if self.store.ingest(ev, commit=False):
                counts["stored"] += 1
            else:
                counts["duplicates"] += 1
            batch.append(path)
            if len(batch) >= batch_size:
                flush()
        flush()
        return counts

    def watch(self, interval: float = 5.0, stop: Callable[[], bool] = lambda: False) -> None:
        if not self.done:
            raise ValueError("watch mode needs done_folder so files are not read twice")
        while not stop():
            c = self.run_once()
            if any(c.values()):
                log.info("ingested %s", c)
            time.sleep(interval)


class MQCollector:
    """Durable subscriber on SYSTEM.FTE for transfer log messages.

    The topic string Log/# matches the transfer log of every agent that uses this
    coordination queue manager. Use Log/AGENT_NAME/# to follow one agent.
    """

    def __init__(self, store: HistoryStore, qmgr: str, channel: str, conn_info: str,
                 topic_string: str = "Log/#", sub_name: str = "MFTHISTORY.SUB",
                 user: Optional[str] = None, password: Optional[str] = None, keep_raw: bool = False):
        self.store = store
        self.qmgr_name = qmgr
        self.channel = channel
        self.conn_info = conn_info
        self.topic_string = topic_string
        self.sub_name = sub_name
        self.user = user
        self.password = password
        self.keep_raw = keep_raw

    def messages(self, wait_ms: int = 5000) -> Iterator[bytes]:  # pragma: no cover - needs MQ
        try:
            import pymqi
        except ImportError as exc:
            raise SystemExit("MQCollector needs pymqi: pip install 'mqmft-transfer-history[mq]'") from exc

        kwargs = {}
        if self.user:
            kwargs = {"user": self.user, "password": self.password}
        qmgr = pymqi.connect(self.qmgr_name, self.channel, self.conn_info, **kwargs)
        sub_desc = pymqi.SD()
        sub_desc["Options"] = (pymqi.CMQC.MQSO_CREATE + pymqi.CMQC.MQSO_RESUME
                               + pymqi.CMQC.MQSO_DURABLE + pymqi.CMQC.MQSO_MANAGED)
        sub_desc.set_vs("SubName", self.sub_name)
        sub_desc.set_vs("ObjectString", self.topic_string)
        sub_desc["ObjectName"] = "SYSTEM.FTE"
        sub = pymqi.Subscription(qmgr)
        sub.sub(sub_desc=sub_desc)
        gmo = pymqi.GMO(Options=pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
                        | pymqi.CMQC.MQGMO_SYNCPOINT)
        gmo["WaitInterval"] = wait_ms
        try:
            while True:
                md = pymqi.MD()
                try:
                    body = sub.get(None, md, gmo)
                except pymqi.MQMIError as e:
                    if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        continue
                    raise
                yield body
                # Commit only after the store accepted the message, so nothing is lost.
                qmgr.commit()
        finally:
            sub.close(sub_close_options=pymqi.CMQC.MQCO_KEEP_SUB, close_sub_queue=True)
            qmgr.disconnect()

    def run(self) -> None:  # pragma: no cover - needs MQ
        for body in self.messages():
            try:
                ev = parse_transfer_log(body, keep_raw=self.keep_raw)
            except ParseError as exc:
                log.warning("skipping message: %s", exc)
                continue
            self.store.ingest(ev)
