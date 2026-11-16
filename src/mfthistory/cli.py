"""Command line entry point: mfthistory <command>."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from . import __version__
from .auth import OpenRegistry, TokenRegistry
from .collectors import DirectoryCollector, MQCollector
from .simulate import SimConfig, write_folder
from .store import HistoryStore


def _store(args) -> HistoryStore:
    return HistoryStore(args.db)


def cmd_simulate(args):
    n = write_folder(args.out, SimConfig(agents=args.agents, transfers=args.transfers, days=args.days, seed=args.seed))
    print(f"wrote {n} synthetic transfer log messages to {args.out}")


def cmd_ingest(args):
    store = _store(args)
    c = DirectoryCollector(store, args.folder, done_folder=args.done, error_folder=args.errors, keep_raw=args.keep_raw)
    if args.watch:
        c.watch(interval=args.interval)
    else:
        print(json.dumps(c.run_once()))


def cmd_collect_mq(args):  # pragma: no cover - needs MQ
    store = _store(args)
    MQCollector(store, qmgr=args.qmgr, channel=args.channel, conn_info=args.conn, topic_string=args.topic,
                sub_name=args.sub_name, user=args.user, password=os.environ.get("MFTHISTORY_MQ_PASSWORD"),
                keep_raw=args.keep_raw).run()


def cmd_search(args):
    store = _store(args)
    res = store.search(transfer_id=args.transfer_id, agent=args.agent, file=args.file, status=args.status,
                       job=args.job, since=args.since, until=args.until, limit=args.limit)
    if args.json:
        print(json.dumps(res, indent=2))
        return
    print(f"{res['total']} transfers")
    for r in res["results"]:
        print(f"{r['started_at']}  {r['status']:<11} {r['source_agent'] or '':<28} -> {r['destination_agent'] or '':<28}"
              f" {r['job_name'] or '':<26} {r['transfer_id']}")


def cmd_show(args):
    row = _store(args).get(args.transfer_id)
    if row is None:
        sys.exit("transfer not found")
    print(json.dumps(row, indent=2))


def cmd_stuck(args):
    for r in _store(args).stuck(args.minutes):
        print(f"{r['started_at']}  {r['source_agent']} -> {r['destination_agent']}  {r['job_name']}  {r['transfer_id']}")


def cmd_stats(args):
    print(json.dumps(_store(args).stats(since=args.since), indent=2))


def cmd_purge(args):
    print(f"deleted {_store(args).purge(args.days)} transfers older than {args.days} days")


def cmd_token(args):
    reg = TokenRegistry(args.tokens)
    if args.action == "add":
        agents = "*" if args.agents == "*" else [a.strip() for a in args.agents.split(",") if a.strip()]
        token = reg.add(args.name, agents)
        reg.save()
        print(f"token for {args.name}: {token}\nstore it now, it is not shown again")
    elif args.action == "revoke":
        n = reg.revoke(args.name)
        reg.save()
        print(f"revoked {n} token(s) for {args.name}")
    else:
        for v in reg.tokens.values():
            print(f"{v['name']}: {v['agents']}")


def cmd_serve(args):
    store = _store(args)
    if args.no_auth:
        if args.host not in ("127.0.0.1", "localhost", "::1"):
            sys.exit("--no-auth is only allowed on localhost")
        registry = OpenRegistry()
    else:
        registry = TokenRegistry(args.tokens)
        if not registry.tokens:
            sys.exit(f"no tokens in {args.tokens}; create one with: mfthistory token add --name admins --agents '*'")
    httpd = __import__("mfthistory.server", fromlist=["serve"]).serve(store, registry, args.host, args.port)
    print(f"serving on http://{args.host}:{httpd.server_address[1]}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


def main(argv=None):
    p = argparse.ArgumentParser(prog="mfthistory", description="Searchable transfer history for IBM MQ MFT")
    p.add_argument("--version", action="version", version=__version__)
    p.add_argument("--db", default=os.environ.get("MFTHISTORY_DB", "mfthistory.db"), help="SQLite database file")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("simulate", help="write synthetic transfer log messages to a folder")
    s.add_argument("--out", default="sample-messages")
    s.add_argument("--agents", type=int, default=30)
    s.add_argument("--transfers", type=int, default=5000)
    s.add_argument("--days", type=int, default=3)
    s.add_argument("--seed", type=int, default=7)
    s.set_defaults(func=cmd_simulate)

    s = sub.add_parser("ingest", help="load transfer log XML files from a folder")
    s.add_argument("folder")
    s.add_argument("--done", help="move processed files here (required with --watch)")
    s.add_argument("--errors", help="move unreadable files here")
    s.add_argument("--keep-raw", action="store_true", help="keep the original XML for audit")
    s.add_argument("--watch", action="store_true")
    s.add_argument("--interval", type=float, default=5.0)
    s.set_defaults(func=cmd_ingest)

    s = sub.add_parser("collect-mq", help="durable subscription to SYSTEM.FTE (needs pymqi)")
    s.add_argument("--qmgr", required=True, help="coordination queue manager")
    s.add_argument("--channel", required=True)
    s.add_argument("--conn", required=True, help="host(port)")
    s.add_argument("--topic", default="Log/#")
    s.add_argument("--sub-name", default="MFTHISTORY.SUB")
    s.add_argument("--user", help="password is read from MFTHISTORY_MQ_PASSWORD")
    s.add_argument("--keep-raw", action="store_true")
    s.set_defaults(func=cmd_collect_mq)

    s = sub.add_parser("search", help="search transfers")
    for k in ("transfer-id", "agent", "file", "status", "job", "since", "until"):
        s.add_argument(f"--{k}")
    s.add_argument("--limit", type=int, default=20)
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_search)

    s = sub.add_parser("show", help="one transfer with files and messages")
    s.add_argument("transfer_id")
    s.set_defaults(func=cmd_show)

    s = sub.add_parser("stuck", help="transfers started but not finished")
    s.add_argument("--minutes", type=float, default=60)
    s.set_defaults(func=cmd_stuck)

    s = sub.add_parser("stats", help="counts by status, day and agent")
    s.add_argument("--since")
    s.set_defaults(func=cmd_stats)

    s = sub.add_parser("purge", help="delete history older than N days")
    s.add_argument("--days", type=float, required=True)
    s.set_defaults(func=cmd_purge)

    s = sub.add_parser("token", help="manage access tokens")
    s.add_argument("action", choices=["add", "revoke", "list"])
    s.add_argument("--name")
    s.add_argument("--agents", default="", help="comma separated agent names, or * for all")
    s.add_argument("--tokens", default=os.environ.get("MFTHISTORY_TOKENS", "tokens.json"))
    s.set_defaults(func=cmd_token)

    s = sub.add_parser("serve", help="run the API and self service page")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8080)
    s.add_argument("--tokens", default=os.environ.get("MFTHISTORY_TOKENS", "tokens.json"))
    s.add_argument("--no-auth", action="store_true", help="demo mode on localhost, everyone is admin")
    s.set_defaults(func=cmd_serve)

    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(asctime)s %(name)s %(message)s")
    args.func(args)


if __name__ == "__main__":
    main()
