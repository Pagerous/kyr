import abc
import asyncio
import base64
from collections import defaultdict
from datetime import datetime
from typing import Iterable

import aiohttp
import requests

from kyr.service.pull.schemas import OrganizationSchema, RepoSchema


class GitHost(abc.ABC):
    NAME = "git_host"

    @abc.abstractmethod
    def get_organization_data(
        self, org_name: str
    ) -> tuple[OrganizationSchema, int]:
        pass

    @abc.abstractmethod
    async def get_all_repos_data(
        self,
        org_name: str,
        repos_count: int,
    ) -> tuple[dict[str, RepoSchema] | None, bool]:
        pass

    @abc.abstractmethod
    async def get_repos_data_by_name(
        self,
        org_name: str,
        repo_names: Iterable[str],
    ) -> dict[str, RepoSchema | None]:
        pass

    @abc.abstractmethod
    async def get_files_from_repos(
        self,
        org_name: str,
        repo_names: Iterable[str],
        file_paths: Iterable[str],
    ) -> dict[dict]:
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
            data = None
        else:
            data = response.json()
            data = {
                "name": data["login"],
                "git_host": "github",
                "private_repos": data["total_private_repos"],
                "public_repos": data["public_repos"],
            }
        return data, response.status_code

    async def get_all_repos_data(
        self,
        org_name: str,
        repos_count: int,
    ) -> tuple[dict[str, RepoSchema] | None, bool]:
        result = {}
        forbidden = False
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
            for response, status_code in task_results:
                if status_code == 403:
                    forbidden = True
                    break
                elif status_code == 200 and len(response) > 0:
                    result.update({item["name"]: item for item in response})
        
            if forbidden:
                return None, forbidden
            
            tasks = []
            for repo_name in result:
                tasks.append(
                    asyncio.create_task(
                        self._get_repo_last_commit_date(
                            session, org_name, repo_name
                        )
                    )
                )
            task_results = await asyncio.gather(*tasks)
            for last_commit_at, status_code, repo_name in task_results:
                if status_code == 403:
                    forbidden = True
                    break
                elif status_code == 200:
                    result[repo_name]["last_commit_at"] = last_commit_at
                
        if forbidden:
            return None, forbidden
            
        return {
            repo_name: (
                {
                    "name": response["name"],
                    "org_name": org_name,
                    "git_host": "github",
                    "created_at": datetime.fromisoformat(
                        response["created_at"]
                    ).replace(tzinfo=None),
                    "last_commit_at": datetime.fromisoformat(
                        response["last_commit_at"]
                    ).replace(tzinfo=None),
                    "html_url": response["html_url"],
                    "api_url": response["url"],
                },
                200,
            )
            for repo_name, response in result.items()
        }, forbidden

    async def get_repos_data_by_name(
        self,
        org_name,
        repo_names: Iterable[str],
    ) -> dict[str, tuple[RepoSchema | None, int]]:
        result = {}
        async with aiohttp.ClientSession() as session:
            tasks = []
            for repo_name in repo_names:
                tasks.append(
                    asyncio.create_task(
                        self._get_repo_data(session, org_name, repo_name)
                    )
                )
            task_results = await asyncio.gather(*tasks)
            for response, status_code, repo_name in task_results:
                result[repo_name] = [response, status_code]
            
            tasks = []
            for repo_name, (_, status_code) in result.items():
                if status_code == 200:
                    tasks.append(
                        asyncio.create_task(
                            self._get_repo_last_commit_date(
                                session, org_name, repo_name
                            )
                        )
                    )
            task_results = await asyncio.gather(*tasks)
            for last_commit_at, status_code, repo_name in task_results:
                if status_code == 403:
                    result[repo_name] = [None, status_code, None]
                elif status_code == 200:
                    result[repo_name].append(last_commit_at)
                
        return {
            repo_name: (
                {
                    "name": response["name"],
                    "org_name": org_name,
                    "git_host": "github",
                    "created_at": datetime.fromisoformat(
                        response["created_at"]
                    ).replace(tzinfo=None),
                    "last_commit_at": datetime.fromisoformat(
                        last_commit_at
                    ).replace(tzinfo=None),
                    "html_url": response["html_url"],
                    "api_url": response["url"],
                },
                status_code,
            )
            if response is not None
            else (None, status_code)
            for (
                repo_name,
                (response, status_code, last_commit_at)
            ) in result.items()
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
            if response.status != 200:
                data = None
            else:
                data = await response.json()
            return data, response.status

    async def _get_repo_data(
        self, session, org_name: str, repo_name: str
    ) -> tuple[dict | None, int, str]:
        async with session.get(
            self._get_url(f"repos/{org_name}/{repo_name}"),
            headers=self._get_headers(),
        ) as response:
            if response.status != 200:
                data = None
            else:
                data = await response.json()
            return data, response.status, repo_name
        
    async def _get_repo_last_commit_date(
        self, session, org_name: str, repo_name: str
    ) -> tuple[str | None, int, str]:
        async with session.get(
            self._get_url(f"repos/{org_name}/{repo_name}/commits"),
            headers=self._get_headers(),
        ) as response:
            if response.status != 200:
                last_commit_at = None
            else:
                body = await response.json()
                last_commit_at = body[0]["commit"]["committer"]["date"]
            return last_commit_at, response.status, repo_name

    async def get_files_from_repos(
        self,
        org_name: str,
        repo_names: Iterable[str],
        file_paths: Iterable[str],
    ) -> dict[dict]:
        result = defaultdict(dict)
        async with aiohttp.ClientSession() as session:
            tasks = []
            for repo_name in repo_names:
                for file_path in file_paths:
                    tasks.append(
                        asyncio.create_task(
                            self._get_file_from_repo(
                                session,
                                org_name=org_name,
                                repo_name=repo_name,
                                file_path=file_path,
                            )
                        )
                    )
            task_results = await asyncio.gather(*tasks)
            for content, status_code, repo_name, file_path in task_results:
                result[file_path][repo_name] = (content, status_code)
        return result

    async def _get_file_from_repo(
        self, session, org_name: str, repo_name: str, file_path: str
    ) -> tuple[bytes | None, int, str, str]:
        async with session.get(
            self._get_url(
                f"repos/{org_name}/{repo_name}/contents/{file_path}"
            ),
            headers=self._get_headers(),
        ) as response:
            if response.status != 200:
                content = None
            else:
                body = await response.json()
                content = base64.b64decode(body["content"])
            return content, response.status, repo_name, file_path
