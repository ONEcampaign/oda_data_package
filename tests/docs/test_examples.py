"""Every code example in the README and the docs site uses real names and runs.

A fenced ``python`` block is an example. ``test_examples_name_real_things``
compiles each one and checks every ``oda.<name>``, ``oda.groups.<name>``, and
``oda.sectors.<name>`` it uses, and every indicator id on the page, prose
included, against the package. ``test_examples_run`` executes a page's
examples in order in one namespace, offline against the cached OECD tables,
from a temporary folder that holds ``oda-cache``, a link to the cache, for
examples that pass ``cache_dir="oda-cache"``.

A fenced ``py`` block is a signature listing: it compiles and does not run.
An example that imports matplotlib only compiles when matplotlib is missing.
"""

import ast
import importlib.util
import os
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

import oda_data as oda
from oda_data import _context
from oda_data._registry import INDICATORS

ROOT = Path(__file__).resolve().parents[2]
PAGES = [ROOT / "README.md", *sorted((ROOT / "docs" / "docs").rglob("*.md"))]

_FENCE = re.compile(r"^```(python|py)\n(.*?)^```$", re.MULTILINE | re.DOTALL)
#: An indicator id in backticks or quotes, as prose and examples write them.
_ID = re.compile(r"[`\"'](o(?:ecd|ne)\.[a-z0-9]+\.[a-z0-9_]+)[`\"']")
#: Misspelt ids that a page uses on purpose, to show the error they raise.
_MISSPELT = {"oecd.dac1.totl_oda"}
_NAMESPACES = {"groups": oda.groups, "sectors": oda.sectors}


def _name(page: Path) -> str:
    return str(page.relative_to(ROOT))


def _blocks(page: Path) -> list[tuple[str, str]]:
    return [(kind, body) for kind, body in _FENCE.findall(page.read_text())]


def _oda_names(tree: ast.AST) -> Iterator[tuple[str, ...]]:
    """Each attribute chain on ``oda`` in ``tree``, such as ``("groups", "g7")``."""
    for node in ast.walk(tree):
        chain: list[str] = []
        current = node
        while isinstance(current, ast.Attribute):
            chain.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name) and current.id == "oda" and chain:
            yield tuple(reversed(chain))


@pytest.mark.parametrize("page", PAGES, ids=_name)
def test_examples_name_real_things(page):
    for kind, body in _blocks(page):
        tree = compile(body, _name(page), "exec", flags=ast.PyCF_ONLY_AST)
        if kind != "python":
            continue
        for chain in _oda_names(tree):
            assert chain[0] in oda.__all__, f"oda.{chain[0]} in {_name(page)}"
            if chain[0] in _NAMESPACES and len(chain) > 1:
                assert chain[1] in _NAMESPACES[chain[0]], (
                    f"oda.{'.'.join(chain[:2])} in {_name(page)}"
                )
    unknown = sorted(set(_ID.findall(page.read_text())) - set(INDICATORS) - _MISSPELT)
    assert not unknown, f"{_name(page)} names unknown indicators {unknown}"


_RUNNABLE = [page for page in PAGES if any(k == "python" for k, _ in _blocks(page))]


@pytest.fixture
def offline_default(tmp_path, monkeypatch):
    """Run from an empty folder, with the default context offline.

    Afterwards the repository root holds the same entries as before, so no
    example writes a file, a chart, or a cache into the repository.
    """
    cache = os.environ.get("ODA_READER_CACHE_DIR")
    if not cache:
        pytest.skip("set ODA_READER_CACHE_DIR to a warm cache (scripts/warm_cache.py)")
    (tmp_path / "oda-cache").symlink_to(Path(cache).expanduser())
    before = {path.name for path in ROOT.iterdir()}
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MPLBACKEND", "Agg")
    context = oda.Context(offline=True)
    monkeypatch.setattr(_context, "_default", context)
    yield
    context.close()
    written = {path.name for path in ROOT.iterdir()} - before
    assert not written, f"the examples wrote {sorted(written)} into the repository"


@pytest.mark.data
@pytest.mark.parametrize("page", _RUNNABLE, ids=_name)
@pytest.mark.usefixtures("offline_default")
def test_examples_run(page):
    has_matplotlib = importlib.util.find_spec("matplotlib") is not None
    namespace: dict[str, object] = {"__name__": "__docs__"}
    for number, (kind, body) in enumerate(_blocks(page), start=1):
        code = compile(body, f"{_name(page)} block {number}", "exec")
        if kind != "python" or ("matplotlib" in body and not has_matplotlib):
            continue
        try:
            exec(code, namespace)
        except oda.OfflineCacheMissError as error:
            pytest.skip(f"cache is cold: {error}")
