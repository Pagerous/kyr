from typing import Iterable

from sqlalchemy import and_, delete, insert, select, update

from kyr.infrastructure.db import models
from kyr.infrastructure.db.session import get_session
from kyr.service import events
from kyr.service.exceptions import MissingDataError
from kyr.service.parsers import PoetryLockParser
from kyr.service.pull.host import GitHost


def _get_organization(
    session, org_name: str, git_host: str
) -> models.Organization | None:
    return session.scalar(
        select(models.Organization)
        .where(models.Organization.name == org_name)
        .where(models.Organization.git_host == git_host)
    )


def pull_organization_data(
    git_host: GitHost, org_name: str
) -> list[events.Event]:
    events_ = []
    data, status_code = git_host.get_organization_data(org_name)
    if status_code == 403:
        events_.append(
            events.OrganizationAccessForbidden(org_name, git_host.NAME)
        )
    else:
        with get_session() as session:
            organization = _get_organization(
                session, org_name=data["name"], git_host=data["git_host"]
            )
            if organization is not None:
                organization.private_repos = data["private_repos"]
                organization.public_repos = data["public_repos"]
            else:
                organization = models.Organization(
                    name=data["name"],
                    git_host=data["git_host"],
                    private_repos=data["private_repos"],
                    public_repos=data["public_repos"],
                )
            events_.append(events.OrganizationUpdated(org_name, git_host))
            session.add(organization)
            session.commit()
    return events_


async def pull_repos_data(
    git_host: GitHost, org_name: str, repo_names: Iterable[str]
):
    events_ = []
    with get_session() as session:
        organization = _get_organization(
            session, org_name=org_name, git_host=git_host.NAME
        )
        if organization is None:
            raise MissingDataError(
                f"unknown organization '{org_name}', "
                "pull it first before continuing"
            )

        if not repo_names:
            data, forbidden = await git_host.get_all_repos_data(
                org_name=org_name,
                repos_count=organization.repos_count,
            )
            if forbidden:
                events_.append(
                    events.ReposListAccessForbidden(
                        org_name=org_name, git_host=git_host
                    )
                )
            repo_names_to_remove = {
                repo_name
                for repo_name in organization.repos
                if repo_name not in data
            }
            repo_names_to_update = {
                repo_name
                for repo_name in data
                if repo_name in organization.repos
                and organization.repos[repo_name].updated_at
                < data[repo_name]["updated_at"]
            }
            repo_names_to_insert = {
                repo_name
                for repo_name in data
                if repo_name not in organization.repos
            }
        else:
            data = await git_host.get_repos_data_by_name(
                org_name=org_name, repo_names=repo_names
            )
            repo_names_not_found = {
                repo_name
                for repo_name, (data_item, status_code) in data.items()
                if data_item is None and status_code == 404
            }
            repo_names_access_rejected = {
                repo_name
                for repo_name, (data_item, status_code) in data.items()
                if data_item is None and status_code == 403
            }
            repo_names_to_remove = {
                repo_name
                for repo_name in organization.repos
                if repo_name in repo_names_not_found
            }
            repo_names_to_update = {
                repo_name
                for repo_name in data
                if repo_name in organization.repos
                and data.get(repo_name, [None, None])[0] is not None
                and organization.repos[repo_name].updated_at
                < data[repo_name][0]["updated_at"]
            }
            repo_names_to_insert = {
                repo_name
                for repo_name in data
                if repo_name not in organization.repos
                and data.get(repo_name, [None, None])[0] is not None
            }
            if repo_names_not_found:
                events_.append(
                    events.ReposNotFound(
                        repo_names_not_found, org_name, git_host
                    )
                )
            if repo_names_access_rejected:
                events_.append(
                    events.ReposAccessForbidden(
                        repo_names_access_rejected, org_name, git_host
                    )
                )
        if repo_names_to_insert:
            session.execute(
                insert(models.Repo),
                [
                    {
                        "name": item["name"],
                        "org_id": organization.id,
                        "created_at": item["created_at"],
                        "updated_at": item["updated_at"],
                        "html_url": item["html_url"],
                        "api_url": item["api_url"],
                    }
                    for item, _ in [
                        data.get(repo_name)
                        for repo_name in repo_names_to_insert
                    ]
                ],
            )
        if repo_names_to_remove:
            session.execute(
                delete(models.Repo).where(
                    and_(
                        models.Repo.name.in_(repo_names_to_remove),
                        models.Repo.org_id == organization.id,
                    )
                )
            )
            events_.append(
                events.ReposRemoved(repo_names_to_remove, org_name, git_host)
            )
        if repo_names_to_update:
            session.execute(
                update(models.Repo),
                [
                    {
                        "id": repo_id,
                        "updated_at": data_item["updated_at"],
                        "html_url": data_item["html_url"],
                        "api_url": data_item["api_url"],
                    }
                    for repo_id, (data_item, _) in [
                        (organization.repos[repo_name].id, data.get(repo_name))
                        for repo_name in repo_names_to_update
                    ]
                ],
            )
        if repo_names_to_insert or repo_names_to_update:
            events_.append(
                events.ReposUpdated(
                    repo_names=repo_names_to_update | repo_names_to_insert,
                    org_name=org_name,
                    git_host=git_host,
                )
            )

        session.commit()
    return events_


async def pull_repo_dependencies(
    git_host: GitHost, org_name: str, repo_names: Iterable[str]
):
    events_ = []
    with get_session() as session:
        organization = _get_organization(
            session, org_name=org_name, git_host=git_host.NAME
        )
        if organization is None:
            raise MissingDataError(
                f"unknown organization '{org_name}', "
                "pull it first before continuing"
            )
        data = await git_host.get_files_from_repos(
            org_name=org_name,
            repo_names=repo_names,
            file_paths=["poetry.lock"],
        )
        existing_python_deps = session.scalars(
            select(models.Dependency).where(
                models.Dependency.language == "python"
            )
        )
        existing_deps = {
            (dep.language, dep.name, dep.version): dep
            for dep in existing_python_deps
        }
        repo_names_access_forbidden = []
        repo_names_dependency_updated = []
        for repo_name, (content, status_code) in data.get(
            "poetry.lock", {}
        ).items():
            dependency_updated = False
            if status_code == 403:
                repo_names_access_forbidden.append(repo_name)
            elif status_code == 200:
                parser = PoetryLockParser(content.decode("utf-8"))
                deps = parser.get_dependencies()
                repo_deps = organization.repos[repo_name].dependencies
                for dep in deps.values():
                    if ("python", dep["name"]) not in repo_deps or repo_deps[
                        ("python", dep["name"])
                    ].version != dep["version"]:
                        dep_key = ("python", dep["name"], dep["version"])
                        if dep_key not in existing_deps:
                            new_dep = models.Dependency(
                                language="python",
                                name=dep["name"],
                                version=dep["version"],
                            )
                            existing_deps[dep_key] = new_dep
                        repo_deps[("python", dep["name"])] = existing_deps[
                            dep_key
                        ]
                        dependency_updated = True
                if dependency_updated:
                    repo_names_dependency_updated.append(repo_name)
        if repo_names_access_forbidden:
            events_.append(
                events.ReposFileAccessForbidden(
                    repo_names=repo_names_access_forbidden,
                    org_name=org_name,
                    git_host=git_host,
                    file_path="poetry.lock",
                )
            )
        if repo_names_dependency_updated:
            events_.append(
                events.ReposDependenciesUpdated(
                    repo_names=repo_names_dependency_updated,
                    org_name=org_name,
                    git_host=git_host,
                )
            )
        session.commit()
    return events_
