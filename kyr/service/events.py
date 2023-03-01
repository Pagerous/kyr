from dataclasses import dataclass

from kyr.service.pull.host import GitHost


@dataclass
class Event:
    pass


@dataclass
class OrganizationUpdated(Event):
    org_name: str
    git_host: GitHost


@dataclass
class OrganizationAccessForbidden(Event):
    org_name: str
    git_host: GitHost


@dataclass
class ReposUpdated(Event):
    repo_names: list[str]
    org_name: str
    git_host: GitHost


@dataclass
class ReposRemoved(Event):
    repo_names: list[str]
    org_name: str
    git_host: GitHost


@dataclass
class ReposNotFound(Event):
    repo_names: list[str]
    org_name: str
    git_host: GitHost


@dataclass
class ReposListAccessForbidden(Event):
    org_name: str
    git_host: GitHost


@dataclass
class ReposAccessForbidden(Event):
    repo_names: list[str]
    org_name: str
    git_host: GitHost


@dataclass
class ReposFileAccessForbidden(Event):
    repo_names: list[str]
    org_name: str
    git_host: GitHost
    file_path: str


@dataclass
class ReposDependenciesUpdated(Event):
    repo_names: list[str]
    org_name: str
    git_host: GitHost
