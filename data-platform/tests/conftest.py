import socket

import pytest


def _up(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("localhost", port)) == 0


def pytest_collection_modifyitems(config, items):
    stack = all(_up(p) for p in (5432, 9000, 8080))
    for item in items:
        if "integration" in item.keywords and not stack:
            item.add_marker(pytest.mark.skip(reason="compose stack is not running (docker compose up -d)"))
