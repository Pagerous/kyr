from dataclasses import dataclass
from datetime import datetime


@dataclass
class Organization:
    name: str
    private_repos: int
    public_repos: int


@dataclass
class Repo:
    name: str
    org_name: str
    created_at: datetime
    updated_at: datetime
    html_url: str
    api_url: str
    updated: bool
    new: bool


@dataclass
class RepoFile:
    repo_name: str
    file_path: str
    content: str


@dataclass
class RepoFull(Repo):
    files: dict[str, RepoFile]
