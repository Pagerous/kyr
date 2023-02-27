import abc
import asyncio
from datetime import datetime
from typing import Iterable

import aiohttp
import requests

from kyr.service.exceptions import PullError
from kyr.service.pull.schemas import OrganizationSchema, RepoSchema


class GitHost(abc.ABC):
    NAME = "git_host"

    @abc.abstractmethod
    def get_organization_data(self, org_name: str) -> OrganizationSchema:
        pass

    @abc.abstractmethod
    async def get_all_repos_data(
        self,
        org_name: str,
        repos_count: int,
    ) -> dict[str, RepoSchema]:
        pass

    @abc.abstractmethod
    async def get_repos_data_by_name(
        self,
        org_name: str,
        repo_names: Iterable[str],
    ) -> dict[str, RepoSchema | None]:
        pass


class GitHub(GitHost):
    NAME = "github"
    BASE_URL = "https://api.github.com"

    def __init__(self, token: str):
        self._token = token

    def _get_headers(self):
        return {"Authorization": f"Bearer {self._token}"}

    @classmethod
    def _get_url(cls, url):
        return f"{cls.BASE_URL}/{url}"

    def get_organization_data(self, org_name: str) -> OrganizationSchema:
        response = requests.get(
            url=self._get_url(f"orgs/{org_name}"), headers=self._get_headers()
        )
        if response.status_code != 200:
            raise PullError(
                f"failed to pull '{org_name}' data for",
                error_code=response.status_code,
            )
        data = response.json()
        return {
            "name": data["login"],
            "git_host": "github",
            "private_repos": data["total_private_repos"],
            "public_repos": data["public_repos"],
        }

    async def get_all_repos_data(
        self,
        org_name: str,
        repos_count: int,
    ) -> dict[str, RepoSchema]:
        result = []
        async with aiohttp.ClientSession() as session:
            tasks = []
            last_page = int(repos_count / 100)
            last_page = last_page if repos_count % 100 == 0 else last_page + 1
            for page in range(1, last_page + 1):
                tasks.append(
                    asyncio.create_task(
                        self._get_repos_data(session, org_name, page)
                    )
                )
            task_results = await asyncio.gather(*tasks)
            for task_result in task_results:
                if len(task_result) > 0:
                    result.extend(
                        [
                            (response["name"], response)
                            for response in task_result
                        ]
                    )
        return {
            repo_name: {
                "name": response["name"],
                "org_name": org_name,
                "git_host": "github",
                "created_at": datetime.fromisoformat(response["created_at"]),
                "updated_at": datetime.fromisoformat(response["updated_at"]),
                "html_url": response["html_url"],
                "api_url": response["url"],
            }
            for repo_name, response in result
        }

    async def get_repos_data_by_name(
        self,
        org_name,
        repo_names: Iterable[str],
    ) -> dict[str, RepoSchema | None]:
        result = []
        async with aiohttp.ClientSession() as session:
            tasks = []
            for repo_name in repo_names:
                tasks.append(
                    asyncio.create_task(
                        self._get_repo_data(session, org_name, repo_name)
                    )
                )
            task_results = await asyncio.gather(*tasks)
            for repo_name, response in zip(repo_names, task_results):
                result.append((repo_name, response))
        return {
            repo_name: {
                "name": response["name"],
                "org_name": org_name,
                "git_host": "github",
                "created_at": datetime.fromisoformat(response["created_at"]),
                "updated_at": datetime.fromisoformat(response["updated_at"]),
                "html_url": response["html_url"],
                "api_url": response["url"],
            }
            if response is not None
            else None
            for repo_name, response in result
        }

    async def _get_repos_data(
        self,
        session,
        org_name: str,
        page: int,
    ) -> list[dict]:
        async with session.get(
            self._get_url(f"orgs/{org_name}/repos?page={page}&per_page=100"),
            headers=self._get_headers(),
        ) as response:
            return await response.json()

    async def _get_repo_data(
        self, session, org_name: str, repo_name: str
    ) -> dict | None:
        async with session.get(
            self._get_url(f"repos/{org_name}/{repo_name}"),
            headers=self._get_headers(),
        ) as response:
            if response.status != 200:
                return None
            return await response.json()
