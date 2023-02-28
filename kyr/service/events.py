from dataclasses import dataclass


@dataclass
class Event:
    pass


@dataclass
class OrganizationPulled(Event):
    org_name: str
    git_host: str


@dataclass
class OrganizationUpdated(Event):
    org_name: str
    git_host: str
    
    
@dataclass
class OrganizationAccessForbidden(Event):
    org_name: str
    git_host: str


@dataclass
class ReposPulled(Event):
    repo_names: list[str]
    org_name: str
    git_host: str


@dataclass
class ReposUpdated(Event):
    repo_names: list[str]
    org_name: str
    git_host: str


@dataclass
class ReposRemoved(Event):
    repo_names: list[str]
    org_name: str
    git_host: str


@dataclass
class ReposNotFound(Event):
    repo_names: list[str]
    org_name: str
    git_host: str


@dataclass
class ReposListAccessForbidden(Event):
    org_name: str
    git_host: str


@dataclass
class ReposAccessForbidden(Event):
    repo_names: list[str]
    org_name: str
    git_host: str