import abc
from typing import Any, Collection, Optional, Protocol


class Matchable(Protocol):
    def matches(self, value: Any) -> bool:
        pass


class _And:
    def __init__(self, left: Matchable, right: Matchable):
        self._left = left
        self._right = right

    def matches(self, value: Any) -> bool:
        return bool(self._left.matches(value) and self._right.matches(value))


class _Or:
    def __init__(self, left: Matchable, right: Matchable):
        self._left = left
        self._right = right

    def matches(self, value: Any) -> bool:
        return bool(self._left.matches(value) or self._right.matches(value))


class _Filter(abc.ABC):
    @abc.abstractmethod
    def matches(self, value: Any) -> bool:
        pass

    @property
    @abc.abstractmethod
    def value(self):
        pass

    def __and__(self, other: Matchable) -> '_And':
        return _And(self, other)

    def __or__(self, other: Matchable) -> '_Or':
        return _Or(self, other)


class In(_Filter):
    def __init__(self, obj_: Collection):
        self._obj = obj_

    @property
    def value(self):
        return self._obj

    def matches(self, value: Any) -> bool:
        return value in self._obj


class StartsWith(_Filter):
    def __init__(self, obj_: str):
        self._obj = obj_

    def matches(self, value: str) -> bool:
        return value.startswith(self._obj)

    @property
    def value(self):
        return self._obj


class Equals(_Filter):
    def __init__(self, obj_: Any):
        self._obj = obj_

    def matches(self, value: Any) -> bool:
        return value == self._obj

    @property
    def value(self):
        return self._obj


class RepoFilter:
    def __init__(self, name: Optional[Matchable] = None):
        self._name = name

    def matches(self, name: str) -> bool:
        if self._name is not None:
            return self._name.matches(name)
        return True

    def get_filter(self, param_name) -> _Filter:
        return getattr(self, f"_{param_name}")

