from dataclasses import dataclass

from kyr.service.pull.host import GitHost, DataFetchFailReason


@dataclass
class Event:
    pass


@dataclass
class OrganizationUpdated(Event):
    org_name: str
    git_host: GitHost


@dataclass
class OrganizationPullFailed(Event):
    org_name: str
    git_host: GitHost
    reason: DataFetchFailReason


@dataclass
class ReposUpdated(Event):
    repo_names: set[str]
    org_name: str
    git_host: GitHost


@dataclass
class ReposListingPullFailed(Event):
    org_name: str
    git_host: GitHost
    reason: DataFetchFailReason


@dataclass
class RepoPullFailed(Event):
    repo_name: str
    org_name: str
    git_host: GitHost
    reason: DataFetchFailReason


@dataclass
class RepoFilePullFailed(Event):
    repo_name: str
    org_name: str
    git_host: GitHost
    file_path: str
    reason: DataFetchFailReason


@dataclass
class ReposDependenciesUpdated(Event):
    repo_names: set[str]
    org_name: str
    git_host: GitHost
