import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def payload_ree() -> dict:
    return json.loads((FIXTURES / "ree_precios_2026-11-08.json").read_text(encoding="utf-8"))
