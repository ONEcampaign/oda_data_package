"""The package surface: names, modules, errors, and import cost."""

import ast
import pickle
import subprocess
import sys
from importlib.resources import files
from pathlib import Path

import pytest

import oda_data

ERRORS = [
    "OdaDataError",
    "UnknownIndicatorError",
    "UnknownEntityError",
    "AmbiguousEntityError",
    "DimensionError",
    "BasisRangeError",
    "UnsupportedQueryError",
    "MissingFactorError",
    "DataUnavailableError",
    "OfflineCacheMissError",
    "ReleaseMismatchError",
    "RemovedIn3Error",
    "ImputationConservationError",
    "UnmappedChannelError",
]

PUBLIC_NAMES = sorted(
    [
        "__version__",
        "get",
        "query",
        "records",
        "Query",
        "Context",
        "Provenance",
        "Call",
        "CallGroup",
        "Release",
        "provenance",
        "indicators",
        "search",
        "describe",
        "IndicatorInfo",
        "add_names",
        "Unit",
        "current",
        "constant",
        "Group",
        "GroupTotal",
        "SectorGroup",
        "ReportedAt",
        "total",
        "groups",
        "sectors",
        *ERRORS,
        "OdaDataWarning",
    ]
)


def test_all_is_pinned() -> None:
    assert sorted(oda_data.__all__) == PUBLIC_NAMES


def test_all_matches_the_lazy_table() -> None:
    assert set(oda_data.__all__) == {"__version__", *oda_data._LAZY}


def test_type_checking_imports_match_the_lazy_table() -> None:
    """Checkers read the ``TYPE_CHECKING`` imports; ``__getattr__`` serves the rest."""
    tree = ast.parse(Path(oda_data.__file__).read_text())
    block = next(
        node
        for node in tree.body
        if isinstance(node, ast.If) and ast.unparse(node.test) == "TYPE_CHECKING"
    )
    imported = {
        alias.name: stmt.module
        for stmt in block.body
        if isinstance(stmt, ast.ImportFrom)
        for alias in stmt.names
    }
    assert imported == oda_data._LAZY


def test_every_name_resolves() -> None:
    for name in oda_data.__all__:
        assert getattr(oda_data, name) is not None


def test_unknown_attribute_raises_attribute_error() -> None:
    with pytest.raises(AttributeError) as error:
        _ = oda_data.no_such_name
    assert not isinstance(error.value, oda_data.RemovedIn3Error)


def test_a_2x_name_raises_removed_in_3_without_loading_pandas() -> None:
    code = (
        "import sys, oda_data\n"
        "try:\n"
        "    oda_data.OECDClient\n"
        "except oda_data.RemovedIn3Error as error:\n"
        "    print(error.code, 'oda.get(' in str(error))\n"
        "print('pandas' in sys.modules)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert result.stdout.split() == ["removed_in_3", "True", "False"]


@pytest.mark.parametrize(
    "statement",
    ["from oda_data import ODAData", "import oda_data; oda_data.ODAData"],
)
def test_a_2x_name_names_its_replacement_on_import_and_attribute_read(
    statement: str,
) -> None:
    with pytest.raises(oda_data.RemovedIn3Error) as error:
        exec(statement, {})
    assert "load_indicator" in str(error.value)
    assert not isinstance(error.value, AttributeError)


def test_version_is_the_package_version() -> None:
    assert oda_data.__version__ == "3.0.1"


def test_the_package_ships_type_information() -> None:
    """The PEP 561 marker, which type checkers need to read the annotations."""
    assert files("oda_data").joinpath("py.typed").is_file()


@pytest.mark.parametrize("name", [n for n in PUBLIC_NAMES if n != "__version__"])
def test_public_classes_and_functions_report_the_package_module(name: str) -> None:
    obj = getattr(oda_data, name)
    if isinstance(obj, type) or callable(obj):
        assert obj.__module__ == "oda_data"


@pytest.mark.parametrize(
    "module",
    ["_context", "_discover", "_errors", "_groups", "_provenance", "_units"],
)
def test_public_names_report_the_package_module_before_any_attribute_read(
    module: str,
) -> None:
    # A name read through ``oda_data`` once had its ``__module__`` rewritten on
    # the way, so a type raised or pickled before that read named the private
    # module. A fresh process imports the defining module directly.
    code = (
        "import importlib, types, oda_data\n"
        f"module = importlib.import_module('oda_data.{module}')\n"
        "names = [n for n, m in oda_data._LAZY.items() if m == module.__name__]\n"
        "assert names\n"
        "found = [getattr(module, n) for n in names]\n"
        "bad = [o.__qualname__ for o in found\n"
        "       if isinstance(o, type | types.FunctionType)\n"
        "       and o.__module__ != 'oda_data']\n"
        "print(bad)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert result.stdout.strip() == "[]"


def test_reported_at_is_the_type_sectors_reported_at_returns() -> None:
    from oda_data import ReportedAt

    assert isinstance(oda_data.sectors.reported_at(151), ReportedAt)


def test_indicator_id_is_the_generated_literal() -> None:
    from oda_data import _ids, _types

    assert _types.IndicatorId is _ids.IndicatorId


def _make_error(cls: type[oda_data.OdaDataError]) -> oda_data.OdaDataError:
    if cls is oda_data.UnknownIndicatorError:
        return cls("no such indicator", suggestions=["oecd.dac1.total_oda"])
    if cls is oda_data.UnknownEntityError:
        return cls("no such entity", suggestions=["France"], system="oecd:dotstat")
    if cls is oda_data.AmbiguousEntityError:
        return cls("ambiguous", candidates=["Congo", "Congo, Dem. Rep."])
    return cls("something went wrong")


@pytest.mark.parametrize("name", ERRORS)
def test_error_message_ends_with_its_code(name: str) -> None:
    cls = getattr(oda_data, name)
    error = _make_error(cls)
    assert str(error).endswith(f"[{cls.code}]")


def test_error_codes_are_unique() -> None:
    codes = [getattr(oda_data, name).code for name in ERRORS]
    assert len(set(codes)) == len(codes)


@pytest.mark.parametrize("name", ERRORS)
def test_error_survives_pickle(name: str) -> None:
    cls = getattr(oda_data, name)
    error = _make_error(cls)
    clone = pickle.loads(pickle.dumps(error))
    assert type(clone) is cls
    assert str(clone) == str(error)
    assert clone.args == error.args
    assert clone.__dict__ == error.__dict__


@pytest.mark.parametrize(
    ("name", "builtin"),
    [
        ("UnknownIndicatorError", LookupError),
        ("UnknownEntityError", LookupError),
        ("AmbiguousEntityError", ValueError),
        ("DimensionError", ValueError),
        ("BasisRangeError", ValueError),
        ("UnsupportedQueryError", ValueError),
        ("DataUnavailableError", ConnectionError),
        ("RemovedIn3Error", ImportError),
    ],
)
def test_error_subclasses_a_builtin(name: str, builtin: type[Exception]) -> None:
    assert issubclass(getattr(oda_data, name), builtin)
    assert issubclass(getattr(oda_data, name), oda_data.OdaDataError)


def test_warning_is_a_user_warning() -> None:
    assert issubclass(oda_data.OdaDataWarning, UserWarning)


def test_import_leaves_heavy_packages_out_of_sys_modules() -> None:
    heavy = ["pandas", "duckdb", "pyarrow", "resolvekit", "oda_reader", "pydeflate"]
    code = (
        "import sys, oda_data\n"
        "oda_data.Unit, oda_data.current, oda_data.OdaDataError\n"
        f"print([m for m in {heavy!r} if m in sys.modules])\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "[]"
