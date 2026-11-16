"""Plain data classes shared by the parser, the store and the API."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class TransferItem:
    """One file inside a transfer (a transfer set can hold many files)."""

    source_file: str
    destination_file: Optional[str] = None
    source_size: Optional[int] = None
    destination_size: Optional[int] = None
    mode: Optional[str] = None
    checksum_method: Optional[str] = None
    checksum: Optional[str] = None
    result_code: Optional[int] = None
    supplement: Optional[str] = None


@dataclass
class TransferEvent:
    """One parsed transfer log message.

    action is one of started, progress, completed, cancelled, malformed.
    """

    transfer_id: str
    action: str
    event_time: Optional[str]
    source_agent: Optional[str] = None
    source_qmgr: Optional[str] = None
    destination_agent: Optional[str] = None
    destination_qmgr: Optional[str] = None
    originator_host: Optional[str] = None
    originator_user: Optional[str] = None
    job_name: Optional[str] = None
    result_code: Optional[int] = None
    supplement: Optional[str] = None
    bytes_sent: Optional[int] = None
    items: list[TransferItem] = field(default_factory=list)
    raw_xml: Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("raw_xml", None)
        return d


# MQ MFT result codes that matter most when someone asks "did my file go".
# 0 success, 40 partial success, 41 cancelled, 42 failed. Other codes are kept as is.
RESULT_LABELS = {
    0: "success",
    40: "partial",
    41: "cancelled",
    42: "failed",
}


def status_for(result_code: Optional[int], action: Optional[str]) -> str:
    """Map a result code and last action to a short status a business user understands."""
    if action == "cancelled":
        return "cancelled"
    if result_code is None:
        return "in_progress" if action in ("started", "progress") else "unknown"
    return RESULT_LABELS.get(result_code, "failed")
