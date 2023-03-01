from typing import TypedDict

from dparse import filetypes, parse


class DependencyData(TypedDict):
    name: str
    version: str


class PoetryLockParser:
    file_type = filetypes.poetry_lock

    def __init__(self, content: str):
        self._parsed_content = parse(content, filetypes.poetry_lock)

    def get_dependencies(self) -> dict[str, DependencyData]:
        return {
            dep.name: {
                "name": dep.name,
                "version": dep.line.split("==")[1],
            }
            for dep in self._parsed_content.dependencies
        }
