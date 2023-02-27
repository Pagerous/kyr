from typing import Iterable

from sqlalchemy import and_, delete, insert, update

from kyr.infrastructure.db import models
from kyr.infrastructure.db.session import get_session
from kyr.service import events
from kyr.service.exceptions import MissingDataError
from kyr.service.pull.host import GitHost


def pull_organization_data(
    git_host: GitHost, org_name: str
) -> list[events.Event]:
    data = git_host.get_organization_data(org_name)
    events_ = []
    with get_session() as session:
        organization = session.get(
            models.Organization, (data["name"], data["git_host"])
        )
        if organization is not None:
            organization.private_repos = data["private_repos"]
            organization.public_repos = data["public_repos"]
            events_.append(events.OrganizationUpdated(org_name, git_host.NAME))
        else:
            organization = models.Organization(
                name=data["name"],
                git_host=data["git_host"],
                private_repos=data["private_repos"],
                public_repos=data["public_repos"],
            )
            session.add(organization)
            events_.append(events.OrganizationPulled(org_name, git_host.NAME))
        session.commit()
    return events_


async def pull_repos_data(
    git_host: GitHost, org_name: str, repo_names: Iterable[str]
):
    events_ = []
    with get_session() as session:
        organization = session.get(
            models.Organization, (org_name, git_host.NAME)
        )
        if organization is None:
            raise MissingDataError(
                f"unknown organization '{org_name}', "
                "pull it first before continuing"
            )

        if not repo_names:
            data = await git_host.get_all_repos_data(
                org_name=org_name,
                repos_count=organization.repos_count,
            )
            repo_names_to_remove = {
                repo_name
                for repo_name in organization.repos
                if repo_name not in data
            }
            repo_names_to_update = {
                repo_name
                for repo_name in organization.repos
                if repo_name in data
            }
            repo_names_to_insert = {
                repo_name
                for repo_name in data
                if repo_name not in repo_names_to_update
            }
        else:
            data = await git_host.get_repos_data_by_name(
                org_name=org_name, repo_names=repo_names
            )
            repo_names_not_found = {
                repo_name
                for repo_name, data_item in data.items()
                if data_item is None
            }
            repo_names_to_remove = {
                repo_name
                for repo_name in organization.repos
                if repo_name in repo_names_not_found
            }
            repo_names_to_update = {
                repo_name
                for repo_name in organization.repos
                if data.get(repo_name) is not None
            }
            repo_names_to_insert = {
                repo_name
                for repo_name, data_item in data.items()
                if data_item is not None
                and repo_name not in repo_names_to_update
            }
            events_.append(
                events.ReposNotFound(
                    repo_names_not_found, org_name, git_host.NAME
                )
            )
        if repo_names_to_insert:
            session.execute(
                insert(models.Repo),
                [data.get(repo_name) for repo_name in repo_names_to_insert],
            )
            events_.append(
                events.ReposPulled(
                    repo_names_to_insert, org_name, git_host.NAME
                )
            )
        if repo_names_to_remove:
            session.execute(
                delete(models.Repo).where(
                    and_(
                        models.Repo.name.in_(repo_names_to_remove),
                        models.Repo.org_name == org_name,
                        models.Repo.git_host == git_host.NAME,
                    )
                )
            )
            events_.append(
                events.ReposRemoved(
                    repo_names_to_remove, org_name, git_host.NAME
                )
            )
        if repo_names_to_update:
            session.execute(
                update(models.Repo),
                [
                    {
                        "name": data_item["name"],
                        "org_name": data_item["org_name"],
                        "git_host": data_item["git_host"],
                        "updated_at": data_item["updated_at"],
                        "html_url": data_item["html_url"],
                        "api_url": data_item["api_url"],
                    }
                    for data_item in [
                        data.get(repo_name)
                        for repo_name in repo_names_to_update
                    ]
                ],
            )
            events_.append(
                events.ReposUpdated(
                    repo_names_to_update, org_name, git_host.NAME
                )
            )

        session.commit()
    return events_
