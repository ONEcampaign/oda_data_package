"""Values a ``Context`` computes once per table release.

A held release does not change for the life of a ``Context``, so a value read
from it, such as the distinct providers of a table or its last year, holds as
long. The ``Context`` owns one ``Store``, creates it with its connection, and
clears it in ``close()``. A call reads the store through ``Releases``, which
keys each value by the table it reads and the release token the call holds.
A new release misses, so a stale value cannot reach a call.

Most values are small and stay until ``close()``. A large one, such as the
channel spending a ``Method`` reads (about 10 MB for nine CRS years), goes in
a section that holds at most ``Store.max_large`` values and drops the least
recently used.

Code that runs without a ``Context``, as unit tests that pass a bare cursor,
reads through ``UNCACHED``, which computes every value.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Mapping

    from oda_data._types import TableName


class Store:
    """Values keyed by what determines them, for the life of a ``Context``.

    Safe across threads. A value is computed outside the lock, so two calls
    that miss at once may both compute it, and the first stored wins. A
    computation that raises stores nothing.

    Args:
        max_large: The most large values held at once.
    """

    def __init__(self, *, max_large: int = 8) -> None:
        self.max_large = max_large
        self._lock = threading.Lock()
        self._values: dict[Hashable, object] = {}
        self._large: OrderedDict[Hashable, object] = OrderedDict()

    def get[T](
        self, key: Hashable, compute: Callable[[], T], *, large: bool = False
    ) -> T:
        """The value under ``key``, computed and kept on the first call.

        Args:
            key: What determines the value.
            compute: Computes the value on a miss.
            large: Whether the value goes in the section that holds the
                ``max_large`` most recently used values. Other values stay
                until ``clear()``.
        """
        values = self._large if large else self._values
        with self._lock:
            if key in values:
                if large:
                    self._large.move_to_end(key)
                return cast("T", values[key])
        value = compute()
        with self._lock:
            value = values.setdefault(key, value)
            if large:
                self._large.move_to_end(key)
                while len(self._large) > self.max_large:
                    self._large.popitem(last=False)
            return cast("T", value)

    def clear(self) -> None:
        """Drop every value."""
        with self._lock:
            self._values.clear()
            self._large.clear()


@dataclass(frozen=True, slots=True)
class Releases:
    """A call's view of a ``Store``: the release token of each table it holds.

    Args:
        store: The Context's store, or ``None`` to compute every value.
        tokens: The release token of each table the call reads.
    """

    store: Store | None = None
    tokens: Mapping[TableName, str] = field(default_factory=dict)

    def get[T](
        self,
        table: TableName,
        name: Hashable,
        compute: Callable[[], T],
        *,
        large: bool = False,
    ) -> T:
        """The value ``name`` read from ``table``'s held release.

        ``large`` as in ``Store.get``.

        Raises:
            KeyError: The call holds no release of ``table``.
        """
        if self.store is None:
            return compute()
        key = (table, self.tokens[table], name)
        return self.store.get(key, compute, large=large)


#: Computes every value, for code that runs without a ``Context``.
UNCACHED = Releases()
