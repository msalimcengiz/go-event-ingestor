"""Tiny helpers for the guided demo. Standard library only, so it runs in a plain
python:3.13-alpine container on the compose network with nothing to install."""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Callable

GREEN, RED, DIM, BOLD, RESET = "\033[32m", "\033[31m", "\033[2m", "\033[1m", "\033[0m"
# Numbering continues across the demo's phases (run.sh passes the next number in).
_steps = int(os.environ.get("DEMO_STEP", "1")) - 1


def step(title: str) -> None:
    global _steps
    _steps += 1
    print(f"\n{BOLD}{_steps}. {title}{RESET}")


def note(text: str) -> None:
    print(f"   {DIM}{text}{RESET}")


def ok(cond: bool, what: str) -> None:
    if cond:
        print(f"   {GREEN}✓{RESET} {what}")
        return
    print(f"   {RED}✗ {what}{RESET}")
    sys.exit(1)


class Response:
    def __init__(self, status: int, body: bytes, headers: dict[str, str]):
        self.status = status
        self.body = body
        self.headers = {k.lower(): v for k, v in headers.items()}

    def json(self) -> Any:
        return json.loads(self.body or b"null")

    def __repr__(self) -> str:
        return f"{self.status} {self.body[:300].decode(errors='replace')}"


def http(method: str, url: str, body: Any = None, headers: dict[str, str] | None = None,
         raw: bytes | None = None, timeout: float = 30) -> Response:
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    h = {"Content-Type": "application/json"} if body is not None else {}
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return Response(r.status, r.read(), dict(r.headers))
    except urllib.error.HTTPError as e:
        return Response(e.code, e.read(), dict(e.headers))


def show(r: Response, max_len: int = 400) -> None:
    text = r.body.decode(errors="replace")
    try:
        text = json.dumps(json.loads(text), ensure_ascii=False)
    except ValueError:
        pass
    print(f"   {DIM}→ {r.status} {text[:max_len]}{'…' if len(text) > max_len else ''}{RESET}")


def wait_until(fn: Callable[[], Any], ok_when: Callable[[Any], bool], timeout: float = 60, every: float = 0.5) -> Any:
    deadline = time.time() + timeout
    last = fn()
    while not ok_when(last):
        if time.time() > deadline:
            print(f"   {RED}✗ timed out; last value: {last!r}{RESET}")
            sys.exit(1)
        time.sleep(every)
        last = fn()
    return last


def next_step() -> int:
    return _steps + 1


def done(summary: str) -> None:
    print(f"\n{GREEN}{BOLD}Demo finished: {summary}{RESET}\n")
