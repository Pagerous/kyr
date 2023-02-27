import asyncio
from datetime import datetime
from typing import Iterable

import aiohttp
import requests

from kyr.service.exceptions import PullError
from kyr.service.pull.schemas import OrganizationSchema, RepoSchema

BASE_GITHUB_URL = "https://api.github.com"


def _get_headers(github_token: str):
    return {"Authorization": f"Bearer {github_token}"}


def get_organization_data(org_name: str, github_token: str) -> OrganizationSchema:
    response = requests.get(
        url=f"{BASE_GITHUB_URL}/orgs/{org_name}", headers=_get_headers(github_token)
    )
    if response.status_code != 200:
        raise PullError(
            f"failed to pull '{org_name}' data for", error_code=response.status_code
        )
    data = response.json()
    return {
        "name": data["login"],
        "git_host": "github",
        "private_repos": data["total_private_repos"],
        "public_repos": data["public_repos"],
    }


async def get_repos_data(
    org_name,
    github_token,
    repo_names: Iterable[str],
    repos_count: int | None = None,
) -> list[RepoSchema]:
    result = []
    async with aiohttp.ClientSession() as session:
        if not repo_names:
            tasks = []
            last_page = int(repos_count / 100)
            last_page = last_page if repos_count % 100 == 0 else last_page + 1
            for page in range(1, last_page + 1):
                tasks.append(
                    asyncio.create_task(
                        _get_repos_data(session, org_name, page, github_token)
                    )
                )
            task_results = await asyncio.gather(*tasks)
            for task_result in task_results:
                if len(task_result) > 0:
                    result.extend(task_result)
        else:
            tasks = []
            for repo_name in repo_names:
                tasks.append(
                    asyncio.create_task(
                        _get_repo_data(session, org_name, repo_name, github_token)
                    )
                )
            task_results = await asyncio.gather(*tasks)
            for response, status_code in task_results:
                if status_code == 200:
                    result.append(response)
                else:
                    ...  # TODO: handle it
    return [
        {
            "name": item["name"],
            "org_name": org_name,
            "git_host": "github",
            "created_at": datetime.fromisoformat(item["created_at"]),
            "updated_at": datetime.fromisoformat(item["updated_at"]),
            "html_url": item["html_url"],
            "api_url": item["url"],
        }
        for item in result
    ]


async def _get_repos_data(
    session, org_name: str, page: int, github_token: str
) -> list[dict]:
    async with session.get(
        f"{BASE_GITHUB_URL}/orgs/{org_name}/repos?page={page}&per_page=100",
        headers=_get_headers(github_token),
    ) as response:
        return await response.json()


async def _get_repo_data(
    session, org_name: str, repo_name: str, github_token: str
) -> tuple[dict | None, int]:
    async with session.get(
        f"{BASE_GITHUB_URL}/repos/{org_name}/{repo_name}",
        headers=_get_headers(github_token),
    ) as response:
        if response.status != 200:
            return None, response.status
        return await response.json(), response.status
