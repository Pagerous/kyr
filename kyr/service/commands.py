from datetime import datetime

from sqlalchemy import select

from kyr.infrastructure.db import models
from kyr.infrastructure.db.session import get_session
from kyr.service import events
from kyr.service.exceptions import MissingDataError
from kyr.service.parsers import PoetryLockParser
from kyr.service.pull.filters import RepoFilter
from kyr.service.pull.host import (
    GitHost,
    DataFetchFailReason,
    DataFetchResult,
    OrganizationResult,
    RepoFullResult,
    ReposListingResult,
)


def pull_organization(
    git_host: GitHost, org_name: str
) -> list[events.Event]:
    events_ = []
    result = git_host.get_organization(org_name)
    if result.fail_reason == DataFetchFailReason.NO_VALID_TOKEN:
        events_.append(
            events.OrganizationPullFailed(org_name=org_name, git_host=git_host.NAME, reason=result.fail_reason)
        )
    else:
        with get_session() as session:
            organization = models.Organization.get(
                session, org_name=result.data.name, git_host_name=git_host.NAME
            )
            if organization is not None:
                organization.private_repos = result.data.private_repos
                organization.public_repos = result.data.public_repos
                organization.system_pulled_at = datetime.utcnow()
            else:
                organization = models.Organization(
                    name=result.data.name,
                    git_host=git_host.NAME,
                    private_repos=result.data.private_repos,
                    public_repos=result.data.public_repos,
                    system_pulled_at=datetime.utcnow(),
                )
            events_.append(events.OrganizationUpdated(org_name, git_host))
            session.add(organization)
            session.commit()
    return events_


async def pull_repos(
    git_host: GitHost, org_name: str, filter_: RepoFilter, n_repos_determined_callback, repo_fetched_callback
):
    events_ = []
    with get_session() as session:
        organization = models.Organization.get(
            session, org_name=org_name, git_host_name=git_host.NAME
        )
        if organization is None:
            raise MissingDataError(
                f"unknown organization '{org_name}', "
                "pull it first before continuing"
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
        repos_last_update = {
            item[0]: item[1] for item in session.execute(select(models.Repo.name, models.Repo.updated_at))
        }
        repos_to_insert, repos_to_update, repos_not_changed, repos_dependency_updated = {}, {}, {}, {}
        pull_datetime = datetime.utcnow()
        broken_fetch = False
        result: DataFetchResult
        async for result in git_host.get_repos(  # noqa
                org_name=org_name,
            repos_last_update=repos_last_update,
            file_paths=["poetry.lock"],
            filter_=filter_,
            n_repos_determined_callback=n_repos_determined_callback,
        ):
            if isinstance(result, OrganizationResult):
                if not result.succeed:
                    events_.append(
                        events.OrganizationPullFailed(
                            org_name=result.org_name,
                            git_host=git_host.NAME,
                            reason=result.fail_reason,
                        )
                    )
                    broken_fetch = True
                    break
            elif isinstance(result, ReposListingResult):
                if not result.succeed:
                    events_.append(
                        events.ReposListingPullFailed(
                            org_name=result.org_name,
                            git_host=git_host.NAME,
                            reason=result.fail_reason,
                        )
                    )
                    broken_fetch = True
                    break
            elif isinstance(result, RepoFullResult):
                repo_fetched_callback()
                if not result.succeed:
                    skip_failed = False
                    for file_result in result.file_results:
                        if file_result.fail_reason in [
                            DataFetchFailReason.NO_VALID_TOKEN, DataFetchFailReason.UNEXPECTED_STATUS
                        ]:
                            skip_failed = True
                            events_.append(
                                events.RepoFilePullFailed(
                                    repo_name=result.repo_name,
                                    org_name=result.org_name,
                                    git_host=git_host.NAME,
                                    file_path=file_result.file_path,
                                    reason=file_result.fail_reason
                                )
                            )
                    if result.fail_reason is not None:
                        skip_failed = True
                        events_.append(
                            events.RepoPullFailed(
                                repo_name=result.repo_name,
                                org_name=result.org_name,
                                git_host=git_host.NAME,
                                reason=result.fail_reason
                            )
                        )

                    if skip_failed:
                        continue

                new_or_updated = False
                if result.data.name not in organization.repos:
                    repo = models.Repo(
                        name=result.data.name,
                        org_id=organization.id,
                        created_at=result.data.created_at,
                        updated_at=result.data.updated_at,
                        system_pulled_at=pull_datetime,
                        html_url=result.data.html_url,
                        api_url=result.data.api_url,
                    )
                    session.add(repo)
                    repos_to_insert[result.data.name] = result.data
                    new_or_updated = True

                elif result.data.updated:
                    repos_to_update[result.data.name] = result.data
                    repo = organization.repos[result.data.name]
                    new_or_updated = True
                else:
                    repos_not_changed[result.data.name] = result.data
                if new_or_updated:
                    dependency_updated = _set_dependencies(repo, result, existing_deps)
                    if dependency_updated:
                        repos_dependency_updated[result.data.name] = result.data

        if not broken_fetch:
            if repos_to_update or repos_not_changed:
                organization.update_repos(
                    session,
                    repos_data=[
                        {
                            "name": data.name,
                            "updated_at": data.updated_at,
                            "system_pulled_at": pull_datetime,
                            "html_url": data.html_url,
                            "api_url": data.api_url,
                        }
                        for data in list(repos_to_update.values()) + list(repos_not_changed.values())
                    ]
                )
            if repos_to_insert or repos_to_update:
                events_.append(
                    events.ReposUpdated(
                        repo_names=set(repos_to_insert | repos_to_update),
                        org_name=org_name,
                        git_host=git_host,
                    )
                )

            if repos_dependency_updated:
                events_.append(
                    events.ReposDependenciesUpdated(
                        repo_names=set(repos_dependency_updated),
                        org_name=org_name,
                        git_host=git_host,
                    )
                )
            session.commit()
        else:
            session.rollback()
    return events_


def _set_dependencies(repo: models.Repo, result: RepoFullResult, existing_deps):
    dependency_updated = False
    for file_path, file_data in result.data.files.items():
        parser = PoetryLockParser(file_data.content)
        deps = parser.get_dependencies()
        repo_deps = repo.dependencies
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
                repo_deps[("python", dep["name"])] = existing_deps[dep_key]
                dependency_updated = True
    return dependency_updated
