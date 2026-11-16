"""API tokens and scopes.

Tokens are random strings handed to a trading partner or a business team. Only a
SHA-256 hash of each token is stored, in a small JSON file:

    {
      "tokens": {
        "<sha256 hex>": {"name": "Acme Logistics", "agents": ["PARTNER_ACME_LOGISTICS"]},
        "<sha256 hex>": {"name": "MFT admins", "agents": "*"}
      }
    }

A scope limited to agents only ever sees transfers where one of those agents was the
source or the destination. That is the rule that makes self service safe to offer.
"""

from __future__ import annotations

import hashlib
import json
import secrets
from pathlib import Path
from typing import Optional

from .store import ADMIN, Scope


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class TokenRegistry:
    def __init__(self, path: Optional[str] = None):
        self.path = Path(path) if path else None
        self.tokens: dict[str, dict] = {}
        if self.path and self.path.exists():
            self.tokens = json.loads(self.path.read_text()).get("tokens", {})

    def save(self) -> None:
        if not self.path:
            raise ValueError("no token file configured")
        self.path.write_text(json.dumps({"tokens": self.tokens}, indent=2, sort_keys=True))
        try:
            self.path.chmod(0o600)
        except OSError:
            pass

    def add(self, name: str, agents: list[str] | str) -> str:
        token = "mfth_" + secrets.token_urlsafe(24)
        self.tokens[hash_token(token)] = {"name": name, "agents": agents}
        return token

    def revoke(self, name: str) -> int:
        doomed = [h for h, v in self.tokens.items() if v.get("name") == name]
        for h in doomed:
            del self.tokens[h]
        return len(doomed)

    def scope_for(self, token: Optional[str]) -> Optional[Scope]:
        if not token:
            return None
        entry = self.tokens.get(hash_token(token))
        if entry is None:
            return None
        agents = entry.get("agents")
        if agents == "*":
            return Scope(entry.get("name", "admin"), None)
        return Scope(entry.get("name", "user"), frozenset(agents or []))


class OpenRegistry(TokenRegistry):
    """No authentication. For local demos only: every caller is an admin."""

    def __init__(self):
        super().__init__(None)

    def scope_for(self, token: Optional[str]) -> Optional[Scope]:
        return ADMIN
