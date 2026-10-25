"""Every link from one doc to another file in this repo goes somewhere."""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
DOCS = sorted([*ROOT.glob("*.md"), *(ROOT / "docs").glob("*.md")])
LINK = re.compile(r"\]\(([^)\s]+)\)")


@pytest.mark.parametrize("doc", DOCS, ids=lambda path: str(path.relative_to(ROOT)))
def test_links_point_to_files_that_exist(doc: Path) -> None:
    broken = []
    for target in LINK.findall(doc.read_text()):
        if target.startswith(("http://", "https://", "#")):
            continue
        path = target.split("#", 1)[0]
        if not (doc.parent / path).exists():
            broken.append(target)
    assert broken == []
