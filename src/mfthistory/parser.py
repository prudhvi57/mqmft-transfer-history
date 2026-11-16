"""Parse IBM MQ MFT transfer log messages.

Agents publish XML transfer log messages to the SYSTEM.FTE topic on the coordination
queue manager (topic strings of the form Log/<agent>/<transfer id>). Each transfer
produces a started message, zero or more progress messages and a completed (or
cancelled) message. The root element is <transaction> and carries the transfer ID.

The parser is deliberately tolerant. It strips XML namespaces, accepts missing
optional elements and never raises on a well formed message that lacks fields, so
one odd message cannot stop a collector. Field names follow the transfer log
message format that IBM documents for MQ MFT. Always test against messages from
your own MQ version before relying on a field.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Optional

from .model import TransferEvent, TransferItem

KNOWN_ACTIONS = {"started", "progress", "completed", "cancelled"}


class ParseError(ValueError):
    """Raised when a message is not XML or is not a transfer log message."""


def _strip_ns(elem: ET.Element) -> None:
    for e in elem.iter():
        if isinstance(e.tag, str) and "}" in e.tag:
            e.tag = e.tag.split("}", 1)[1]
        for k in list(e.attrib):
            if "}" in k:
                e.attrib[k.split("}", 1)[1]] = e.attrib.pop(k)


def _int(v: Optional[str]) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except ValueError:
        try:
            return int(float(v))
        except ValueError:
            return None


def _text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None or elem.text is None:
        return None
    t = elem.text.strip()
    return t or None


def _agent(root: ET.Element, tag: str) -> tuple[Optional[str], Optional[str]]:
    a = root.find(tag)
    if a is None:
        return None, None
    return a.get("agent"), a.get("QMgr") or a.get("qmgr")


def _parse_item(item: ET.Element) -> TransferItem:
    src = item.find("source")
    dst = item.find("destination")
    src_file = src.find("file") if src is not None else None
    if src is not None and src_file is None:
        src_file = src.find("queue")
    dst_file = dst.find("file") if dst is not None else None
    if dst is not None and dst_file is None:
        dst_file = dst.find("queue")
    checksum = src.find("checksum") if src is not None else None
    status = item.find("status")
    return TransferItem(
        source_file=_text(src_file) or "",
        destination_file=_text(dst_file),
        source_size=_int(src_file.get("size")) if src_file is not None else None,
        destination_size=_int(dst_file.get("size")) if dst_file is not None else None,
        mode=item.get("mode"),
        checksum_method=checksum.get("method") if checksum is not None else None,
        checksum=_text(checksum),
        result_code=_int(status.get("resultCode")) if status is not None else None,
        supplement=_text(status.find("supplement")) if status is not None else None,
    )


def parse_transfer_log(xml_text: str | bytes, keep_raw: bool = False) -> TransferEvent:
    """Turn one transfer log message into a TransferEvent."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ParseError(f"not XML: {exc}") from exc
    _strip_ns(root)
    if root.tag != "transaction":
        raise ParseError(f"not a transfer log message (root element {root.tag!r})")
    transfer_id = root.get("ID") or root.get("id")
    if not transfer_id:
        raise ParseError("transfer log message has no transfer ID")

    action_el = root.find("action")
    action = (_text(action_el) or "").lower()
    if action not in KNOWN_ACTIONS:
        action = "malformed" if not action else action
    event_time = action_el.get("time") if action_el is not None else None

    src_agent, src_qm = _agent(root, "sourceAgent")
    dst_agent, dst_qm = _agent(root, "destinationAgent")
    orig = root.find("originator")
    job = root.find("job")

    status = root.find("status")
    tset = root.find("transferSet")
    bytes_sent = None
    if tset is not None:
        bytes_sent = _int(tset.get("bytesSent"))
    items = [_parse_item(i) for i in tset.findall("item")] if tset is not None else []

    return TransferEvent(
        transfer_id=transfer_id,
        action=action,
        event_time=event_time,
        source_agent=src_agent,
        source_qmgr=src_qm,
        destination_agent=dst_agent,
        destination_qmgr=dst_qm,
        originator_host=_text(orig.find("hostName")) if orig is not None else None,
        originator_user=_text(orig.find("userID")) if orig is not None else None,
        job_name=_text(job.find("name")) if job is not None else None,
        result_code=_int(status.get("resultCode")) if status is not None else None,
        supplement=_text(status.find("supplement")) if status is not None else None,
        bytes_sent=bytes_sent,
        items=items,
        raw_xml=(xml_text.decode() if isinstance(xml_text, bytes) else xml_text) if keep_raw else None,
    )
