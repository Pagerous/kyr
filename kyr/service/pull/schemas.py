from datetime import datetime
from typing import TypedDict


class OrganizationSchema(TypedDict):
    name: str
    private_repos: int
    public_repos: int
    git_host: str


class RepoSchema(TypedDict):
    name: str
    org_name: str
    git_host: str
    created_at: datetime
    updated_at: datetime
    html_url: str
    api_url: str
