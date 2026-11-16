"""Synthetic MQ MFT transfer log messages for demos and tests.

Everything here is invented. Agent names, partners, hosts and file names are made
up and do not describe any real company. The shape of the XML follows the MQ MFT
transfer log message format closely enough to exercise the parser and the store.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional
from xml.sax.saxutils import escape

from .timeutil import fmt_ts

PARTNERS = [
    "ACME_LOGISTICS", "BLUEWAVE_BANK", "NORTHSTAR_FOODS", "PIXEL_PAYROLL", "RIVERSIDE_DC",
    "SUMMIT_INSURANCE", "TRILLIUM_RETAIL", "VERTEX_TAX", "WESTFIELD_FREIGHT", "ZENITH_HEALTH",
]
FEEDS = [
    ("INVENTORY", "inv_{d}_{n:04d}.csv", 5, (40_000, 900_000)),
    ("ORDERS", "orders_{d}_{n:03d}.xml", 15, (5_000, 300_000)),
    ("PAYROLL", "payroll_{d}.dat", 1440, (2_000_000, 9_000_000)),
    ("PRICES", "price_update_{d}.txt", 60, (100_000, 2_000_000)),
    ("ASN", "asn_{d}_{n:03d}.edi", 30, (2_000, 80_000)),
    ("INVOICES", "inv810_{d}_{n:03d}.edi", 120, (3_000, 150_000)),
]


@dataclass
class SimConfig:
    agents: int = 30
    transfers: int = 5000
    days: int = 3
    seed: int = 7
    p_failed: float = 0.02
    p_partial: float = 0.01
    p_cancelled: float = 0.005
    p_stuck: float = 0.005
    p_duplicate: float = 0.02
    p_out_of_order: float = 0.01
    end: Optional[datetime] = None


def _agents(n: int) -> list[tuple[str, str]]:
    out = []
    for i in range(n):
        if i < len(PARTNERS) * 2 and i % 2 == 1:
            name = f"PARTNER_{PARTNERS[(i // 2) % len(PARTNERS)]}"
        else:
            name = f"HUB_AGENT_{i:02d}"
        out.append((name, f"QM_MFT_{i % 4 + 1}"))
    return out


def _transfer_id(rng: random.Random) -> str:
    return "414d5120" + "".join(rng.choice("0123456789abcdef") for _ in range(40))


def _xml(tid: str, action: str, ts: datetime, src, dst, job: str, user: str, host: str,
         items: list[dict], result: Optional[int], supplement: Optional[str], with_items: bool) -> str:
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<transaction xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="6.00" ID="{tid}"'
        ' agentRole="sourceAgent" xsi:noNamespaceSchemaLocation="TransferLog.xsd"'
        ' xmlns="">',
        f'  <action time="{fmt_ts(ts)}">{action}</action>',
        f'  <sourceAgent agent="{src[0]}" QMgr="{src[1]}"><systemInfo architecture="amd64" name="Linux" version="5.14"/></sourceAgent>',
        f'  <destinationAgent agent="{dst[0]}" QMgr="{dst[1]}"/>',
        f'  <originator><hostName>{host}</hostName><userID>{user}</userID></originator>',
        f'  <job><name>{escape(job)}</name></job>',
    ]
    if result is not None:
        sup = f"<supplement>{escape(supplement)}</supplement>" if supplement else ""
        parts.append(f'  <status resultCode="{result}">{sup}</status>')
    if with_items:
        total = sum(i["size"] for i in items)
        bytes_attr = f' bytesSent="{total if result in (0, None) else total // 2}"' if action == "completed" else ""
        parts.append(f'  <transferSet startTime="{fmt_ts(ts)}" total="{len(items)}"{bytes_attr}>')
        for it in items:
            st = ""
            if action in ("completed", "cancelled"):
                sup = f"<supplement>{escape(it['supplement'])}</supplement>" if it.get("supplement") else ""
                st = f'<status resultCode="{it["rc"]}">{sup}</status>'
            parts.append(
                f'    <item mode="binary"><source disposition="leave" type="file">'
                f'<file size="{it["size"]}" last-modified="{fmt_ts(ts)}">{escape(it["src"])}</file>'
                f'<checksum method="MD5">{it["md5"]}</checksum></source>'
                f'<destination type="file" exist="overwrite"><file size="{it["size"] if it["rc"] == 0 else 0}">'
                f'{escape(it["dst"])}</file></destination>{st}</item>'
            )
        parts.append("  </transferSet>")
    parts.append("</transaction>")
    return "\n".join(parts)


def generate(cfg: SimConfig = SimConfig()) -> Iterator[tuple[str, str]]:
    """Yield (filename, xml) pairs in roughly the order an agent would publish them."""
    rng = random.Random(cfg.seed)
    agents = _agents(cfg.agents)
    hubs = [a for a in agents if a[0].startswith("HUB_")]
    partners = [a for a in agents if a[0].startswith("PARTNER_")] or hubs
    end = cfg.end or datetime.now(timezone.utc).replace(microsecond=0)
    start = end - timedelta(days=cfg.days)
    span = (end - start).total_seconds()

    pending: list[tuple[datetime, str, str]] = []
    for n in range(cfg.transfers):
        feed, pattern, _cadence, (lo, hi) = rng.choice(FEEDS)
        outbound = rng.random() < 0.6
        hub, partner = rng.choice(hubs), rng.choice(partners)
        src, dst = (hub, partner) if outbound else (partner, hub)
        ts = start + timedelta(seconds=rng.random() * span)
        day = ts.strftime("%Y%m%d")
        files = 1 if rng.random() < 0.85 else rng.randint(2, 6)
        items = []
        for k in range(files):
            name = pattern.format(d=day, n=n * 10 + k)
            items.append({
                "src": f"/mft/{'out' if outbound else 'in'}/{feed.lower()}/{name}",
                "dst": f"/data/{dst[0].lower()}/{feed.lower()}/{name}",
                "size": rng.randint(lo, hi),
                "md5": "".join(rng.choice("0123456789abcdef") for _ in range(32)),
                "rc": 0,
            })
        roll = rng.random()
        result, supplement, stuck, action_end = 0, None, False, "completed"
        if roll < cfg.p_failed:
            result = 42
            supplement = rng.choice([
                "BFGIO0001E: File not found", "BFGSS0021E: Destination directory does not exist",
                "BFGTR0072E: The transfer failed to complete due to the exception: Disk full",
                "BFGIO0006E: Permission denied writing destination file",
            ])
            for it in items:
                it["rc"], it["supplement"] = 1, supplement
        elif roll < cfg.p_failed + cfg.p_partial:
            if files > 1:  # a single file transfer cannot partly fail, so it simply succeeds
                result, supplement = 40, "BFGRP0034I: The file transfer request has completed with some failures"
                items[-1]["rc"], items[-1]["supplement"] = 1, "BFGIO0001E: File not found"
        elif roll < cfg.p_failed + cfg.p_partial + cfg.p_cancelled:
            result, supplement, action_end = 41, "BFGRP0036I: The file transfer request was cancelled", "cancelled"
        elif roll < cfg.p_failed + cfg.p_partial + cfg.p_cancelled + cfg.p_stuck:
            stuck = True
        tid = _transfer_id(rng)
        job = f"{feed}_{partner[0].replace('PARTNER_', '')}"
        user, host = "mftadm", f"mfthost{rng.randint(1, 6):02d}.example.internal"
        dur = timedelta(seconds=rng.uniform(1, 90) * files)
        msgs = [(ts, f"{tid}_1_started.xml",
                 _xml(tid, "started", ts, src, dst, job, user, host, items, None, None, True))]
        if files > 2:
            msgs.append((ts + dur / 2, f"{tid}_2_progress.xml",
                         _xml(tid, "progress", ts + dur / 2, src, dst, job, user, host, items[:2], None, None, True)))
        if not stuck:
            msgs.append((ts + dur, f"{tid}_3_{action_end}.xml",
                         _xml(tid, action_end, ts + dur, src, dst, job, user, host, items, result, supplement, True)))
        if rng.random() < cfg.p_out_of_order and len(msgs) > 1:
            # Deliver the final message before the started message (pub/sub does not promise order).
            t, f, x = msgs[-1]
            msgs[-1] = (msgs[0][0] - timedelta(seconds=1), f, x)
        if rng.random() < cfg.p_duplicate:
            t, f, x = msgs[-1]
            msgs.append((t + timedelta(milliseconds=5), f.replace(".xml", "_dup.xml"), x))
        pending.extend(msgs)

    pending.sort(key=lambda m: m[0])
    for _, fname, xml in pending:
        yield fname, xml


def write_folder(folder: str, cfg: SimConfig = SimConfig()) -> int:
    out = Path(folder)
    out.mkdir(parents=True, exist_ok=True)
    count = 0
    for i, (fname, xml) in enumerate(generate(cfg)):
        (out / f"{i:07d}_{fname}").write_text(xml)
        count += 1
    return count
