from typing import Callable

from sqlalchemy import select

from kyr.infrastructure.db import models
from kyr.infrastructure.db.session import get_session


class DependencyRequirement:
    def __init__(
        self, language: str, name: str, version: str, operator_func: Callable
    ):
        self._language = language
        self._name = name
        if version == "*":
            self._version = version
            self._operator = lambda a, b: True
        else:
            self._version = self._str_version_to_tuple(version)
            self._operator = operator_func

    def match(self, language: str, name: str, version: str) -> bool:
        if language != self._language:
            return False
        if name != self._name:
            return False
        try:
            return self._operator(
                self._str_version_to_tuple(version), self._version
            )
        except:
            return False

    @staticmethod
    def _str_version_to_tuple(version: str):
        return [int(x) for x in version.split(".")]


def get_repos(
    dependency_requirements: dict[tuple[str, str], DependencyRequirement]
) -> list[models.Repo]:
    with get_session() as session:
        repos = session.scalars(select(models.Repo))
        repos_matched = []
        for repo in repos:
            for (
                dependency_key,
                dependency_requirement,
            ) in dependency_requirements.items():
                if dependency_key not in repo.dependencies:
                    break
                repo_dependency = repo.dependencies[dependency_key]
                if not dependency_requirement.match(
                    language=repo_dependency.language,
                    name=repo_dependency.name,
                    version=repo_dependency.version,
                ):
                    break
            else:
                repos_matched.append(repo)
    return repos_matched
