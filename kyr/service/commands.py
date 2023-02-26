from typing import Iterable

from sqlalchemy.dialects.sqlite import insert

from kyr.infrastructure.db.session import get_session
from kyr.infrastructure.db import models
from kyr.service.pull.host import github
from kyr.service.exceptions import GitHostException, MissingDataError


def _validate_git_host(git_host: str):
    if git_host not in models.GitHost.values():
        raise GitHostException(f"unknown git host '{git_host}'")


def pull_organization_data(git_host: str, org_name: str, github_token: str):
    if git_host == models.GitHost.GITHUB:
        data = github.get_organization_data(org_name=org_name, github_token=github_token)
    else:
        raise GitHostException(f"unknown git host '{git_host}'")
    with get_session() as session:
        organization = session.get(models.Organization, (data["name"], data["git_host"]))
        if organization is not None:
            organization.private_repos = data["private_repos"]
            organization.private_repos = data["public_repos"]
        else:
            organization = models.Organization(
                name=data["name"],
                git_host=data["git_host"],
                private_repos=data["private_repos"],
                public_repos=data["public_repos"]
            )
            session.add(organization)
        session.commit()


async def pull_repos_data(git_host: str, org_name: str, github_token: str, repo_names: Iterable[str]):
    _validate_git_host(git_host)
    with get_session() as session:
        organization = session.get(models.Organization, (org_name, git_host))
        if organization is None:
            raise MissingDataError(f"unknown organization '{org_name}', pull it first before continuing")
        if git_host == models.GitHost.GITHUB:
            data = await github.get_repos_data(
                org_name=org_name,
                github_token=github_token,
                repo_names=repo_names,
                repos_count=organization.private_repos + organization.public_repos
            )
        else:
            raise GitHostException(f"unknown git host '{git_host}'")
        session.execute(
            insert(models.Repo).values(data).on_conflict_do_update(
                index_elements=models.Repo.primary_keys(), set_=models.Repo.update_columns()
            )
        )
        session.commit()

