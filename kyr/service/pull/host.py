import abc
import asyncio
import base64
import dataclasses
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Collection, Generator, Iterable, Optional, Union

import aiohttp
import requests

from kyr.service.pull.filters import In, RepoFilter
from kyr.service.pull.schemas import Organization, Repo, RepoFile, RepoFull
from kyr.service.exceptions import GitTokenError, InconsistentResultsError


class GitHubToken:
    EXPIRATION_PERIOD = timedelta(hours=1)

    def __init__(self, token: str, last_expired: Optional[datetime]):
        self._token = token
        self._last_expired = last_expired

    @property
    def token(self) -> str:
        return self._token

    def is_valid(self) -> bool:
        if self._last_expired is None:
            return True
        return (datetime.utcnow() - self._last_expired) >= self.EXPIRATION_PERIOD

    def expire(self) -> None:
        self._last_expired = datetime.utcnow()


class GitHubTokenManager:
    def __init__(self, tokens: Iterable[GitHubToken]):
        self._tokens = tokens

    def get_token(self) -> Optional[GitHubToken]:
        for token in self._tokens:
            if token.is_valid():
                return token
        return None


class DataFetchFailReason(StrEnum):
    NO_VALID_TOKEN = "NO_VALID_TOKEN"
    UNEXPECTED_STATUS = "UNEXPECTED_STATUS"
    NOT_FOUND = "NOT_FOUND"


@dataclass
class DataFetchResult:
    org_name: str
    succeed: bool
    fail_reason: Optional[DataFetchFailReason]


@dataclass
class OrganizationResult(DataFetchResult):
    data: Optional[Organization] = None


@dataclass
class RepoResult(DataFetchResult):
    repo_name: str


@dataclass
class RepoDetailResult(RepoResult):
    data: Optional[Repo] = None


@dataclass
class RepoFileResult(RepoResult):
    file_path: str
    data: Optional[RepoFile] = None


@dataclass
class ReposListingResult(DataFetchResult):
    data: Optional[Repo] = None


class RepoResultMerger:
    def __init__(self, file_paths: Optional[Iterable[str]]):
        self._file_present = {file_path: False for file_path in file_paths or {}}
        self._repo_result: Optional[RepoDetailResult] = None
        self._repo_file_results: dict[str, Optional[RepoFileResult]] = {
            file_path: None for file_path in file_paths or {}
        }

    def add_result(self, result: RepoResult) -> None:
        if isinstance(result, RepoDetailResult):
            if self._repo_result is not None:
                raise InconsistentResultsError(
                    f"the same repo result for '{self._repo_result.repo_name}' has been added twice"
                )
            self._repo_result = result
        elif isinstance(result, RepoFileResult):
            if result.file_path not in self._file_present:
                raise InconsistentResultsError(f"unexpected file '{result.file_path}' has been added")
            elif self._file_present.get(result.file_path) is True:
                raise InconsistentResultsError(
                    f"The same file '{result.file_path}' for '{result.repo_name}' has been added twice"
                )
            else:
                self._file_present[result.file_path] = True
                self._repo_file_results[result.file_path] = result

    def _is_complete(self) -> bool:
        if self._repo_result is not None:
            if self._repo_result.data is None:
                return True
            elif not (self._repo_result.data.new or self._repo_result.data.updated):
                return True
            return all(self._file_present.values())
        return False

    def _is_consistent(self) -> bool:
        repo_name = self._repo_result.repo_name
        org_name = self._repo_result.org_name
        for repo_file_result in self._repo_file_results.values():
            if repo_file_result.repo_name != repo_name or repo_file_result.org_name != org_name:
                return False
        return True

    def get_merged_result(self) -> Optional['RepoFullResult']:
        if not self._is_complete() or not self._is_consistent():
            return None
        repo_data = self._repo_result.data
        merged_data = RepoFull(
            name=repo_data.name,
            org_name=repo_data.org_name,
            created_at=repo_data.created_at,
            updated_at=repo_data.updated_at,
            html_url=repo_data.html_url,
            api_url=repo_data.api_url,
            updated=repo_data.updated,
            new=repo_data.new,
            files={}
        )
        succeed = self._repo_result.succeed
        for file_result in self._repo_file_results.values():
            if not file_result.succeed:
                succeed = False
                break
            else:
                merged_data.files[file_result.file_path] = dataclasses.replace(file_result.data)
        return RepoFullResult(
            org_name=self._repo_result.org_name,
            repo_name=self._repo_result.org_name,
            succeed=succeed,
            fail_reason=self._repo_result.fail_reason,
            file_results=list(self._repo_file_results.values()),
            data=merged_data
        )


@dataclass
class RepoFullResult(DataFetchResult):
    repo_name: str
    file_results: list[RepoFileResult]
    data: Optional[RepoFull] = None


class GitHost(abc.ABC):
    NAME = "git_host"

    @abc.abstractmethod
    def get_organization(
        self, org_name: str
    ) -> OrganizationResult:
        pass

    @abc.abstractmethod
    async def get_repos(
        self,
        org_name: str,
        repos_last_update: dict[str, datetime],
        file_paths: Iterable[str],
        filter_,
    ) -> Generator[DataFetchResult, None, None]:
        pass


class DataFetchRequest(abc.ABC):
    BASE_URL = "https://api.github.com"

    def __init__(self, token_manager: GitHubTokenManager):
        self._token_manager = token_manager
        self._token: GitHubToken = self._token_manager.get_token()
        if self._token is None:
            raise GitTokenError("no valid token")

    def _get_url(self, url):
        return f"{self.BASE_URL}/{url}"

    @property
    def headers(self):
        return {"Authorization": f"Bearer {self._token.token}"}

    @property
    @abc.abstractmethod
    def url(self):
        pass

    @abc.abstractmethod
    def _handle_no_token(self, **kwargs) -> tuple[Optional[DataFetchResult], Optional[list['DataFetchRequest']]]:
        pass

    @abc.abstractmethod
    def _handle_invalid_token(
        self, response_body, **kwargs
    ) -> tuple[Optional[DataFetchResult], Optional[list['DataFetchRequest']]]:
        pass

    @abc.abstractmethod
    def _handle_status(
        self, response_status: int, response_body, **kwargs
    ) -> tuple[Optional[DataFetchResult], Optional[list['DataFetchRequest']]]:
        pass

    @abc.abstractmethod
    def _handle_valid_response(
        self, response_body, **kwargs
    ) -> tuple[Optional[DataFetchResult], Optional[list['DataFetchRequest']]]:
        pass

    @abc.abstractmethod
    def _handle_token_retry(
        self, response_body, **kwargs
    ) -> tuple[Optional[DataFetchResult], Optional[list['DataFetchRequest']]]:
        pass

    async def make_with_aiohttp(
        self, session: aiohttp.ClientSession, **kwargs
    ) -> tuple[Optional[DataFetchResult], Optional[list['DataFetchRequest']]]:
        if self._token is None:
            return self._handle_no_token(**kwargs)
        async with session.get(url=self.url, headers=self.headers) as response:
            response_body = await response.json()
            return self._process_response(response.status, response_body, **kwargs)

    def make_with_requests(self, **kwargs) -> tuple[Optional[DataFetchResult], Optional[list['DataFetchRequest']]]:
        if self._token is None:
            return self._handle_no_token(**kwargs)
        response = requests.get(url=self.url, headers=self.headers)
        return self._process_response(response.status_code, response.json(), **kwargs)

    def _process_response(
        self, response_status, response_body, **kwargs
    ) -> tuple[Optional[DataFetchResult], Optional[list['DataFetchRequest']]]:
        if response_status == 403:
            if self._token is None:
                return self._handle_invalid_token(response_body, **kwargs)
            else:
                self._token.expire()
                return self._handle_token_retry(response_body, **kwargs)
        elif response_status != 200:
            return self._handle_status(response_status, response_body, **kwargs)
        return self._handle_valid_response(response_body, **kwargs)


class ReposListingRequest(DataFetchRequest):
    def __init__(
        self,
        token_manager: GitHubTokenManager,
        org_name: str,
        page: int,
        page_size: int,
        filter_: RepoFilter
    ):
        super().__init__(token_manager)
        self._org_name = org_name
        self._page = page
        self._page_size = page_size
        self._filter = filter_

    @property
    def url(self):
        return self._get_url(f"orgs/{self._org_name}/repos?page={self._page}&per_page={self._page_size}")

    def _handle_no_token(self, **kwargs) -> tuple[ReposListingResult, None]:
        return ReposListingResult(
            org_name=self._org_name,
            succeed=False,
            fail_reason=DataFetchFailReason.NO_VALID_TOKEN
        ), None

    def _handle_invalid_token(self, response_body, **kwargs) -> tuple[ReposListingResult, None]:
        return ReposListingResult(
            org_name=self._org_name,
            succeed=False,
            fail_reason=DataFetchFailReason.NO_VALID_TOKEN
        ), None

    def _handle_token_retry(self, response_body, **kwargs) -> tuple[None, list['ReposListingRequest']]:
        return (
            None,
            [
                ReposListingRequest(
                    token_manager=self._token_manager,
                    org_name=self._org_name,
                    page=self._page,
                    page_size=self._page_size,
                    filter_=self._filter,
                )
            ]
        )

    def _handle_status(self, response_status: int, response_body, **kwargs) -> tuple[DataFetchResult, None]:
        return ReposListingResult(
            org_name=self._org_name, succeed=False, fail_reason=DataFetchFailReason.UNEXPECTED_STATUS
        ), None

    def _handle_valid_response(self, response_body, **kwargs) -> tuple[None, list['RepoRequest']]:
        requests_ = []
        repos_last_update: dict[str, datetime] = kwargs.get("repos_last_update", {})
        requests_.extend(
            [
                RepoRequest(
                    token_manager=self._token_manager,
                    org_name=self._org_name,
                    repo_name=item["name"],
                    repo_last_update=repos_last_update.get(item["name"])
                )
                for item in response_body
                if self._filter.matches(name=item["name"])
            ]
        )
        return None, requests_


class RepoRequest(DataFetchRequest):
    def __init__(
        self,
        token_manager: GitHubTokenManager,
        org_name: str,
        repo_name: str,
        repo_last_update: Optional[datetime],
    ):
        super().__init__(token_manager=token_manager)
        self._org_name = org_name
        self._repo_name = repo_name
        self._repo_last_update = repo_last_update

    @property
    def url(self):
        return self._get_url(f"repos/{self._org_name}/{self._repo_name}")

    def _handle_no_token(self, **kwargs) -> tuple[RepoDetailResult, None]:
        return RepoDetailResult(
            org_name=self._org_name,
            repo_name=self._repo_name,
            succeed=False,
            fail_reason=DataFetchFailReason.NO_VALID_TOKEN
        ), None

    def _handle_invalid_token(self, response_body, **kwargs) -> tuple[RepoDetailResult, None]:
        return RepoDetailResult(
            org_name=self._org_name,
            repo_name=self._repo_name,
            succeed=False,
            fail_reason=DataFetchFailReason.NO_VALID_TOKEN
        ), None

    def _handle_token_retry(self, response_body, **kwargs) -> tuple[None, list['RepoRequest']]:
        return (
            None,
            [
                RepoRequest(
                    token_manager=self._token_manager,
                    org_name=self._org_name,
                    repo_name=self._repo_name,
                    repo_last_update=self._repo_last_update,
                )
            ]
        )

    def _handle_status(self, response_status: int, response_body, **kwargs) -> tuple[RepoDetailResult, None]:
        reason = DataFetchFailReason.NOT_FOUND if response_status == 404 else DataFetchFailReason.UNEXPECTED_STATUS
        return RepoDetailResult(
            org_name=self._org_name,
            repo_name=self._repo_name,
            succeed=False,
            fail_reason=reason
        ), None

    def _handle_valid_response(
        self, response_body, **kwargs
    ) -> tuple[RepoDetailResult, list['RepoFileRequest']]:
        last_push_datetime = datetime.fromisoformat(response_body["pushed_at"]).replace(tzinfo=None)
        file_paths: Iterable[str] = kwargs.get("file_paths", [])
        requests_ = []
        if self._repo_last_update is None or last_push_datetime > self._repo_last_update:
            if self._repo_last_update is None:
                new = True
                updated = False
            else:
                new = False
                updated = True
            for file_path in file_paths:
                requests_.append(
                    RepoFileRequest(
                        token_manager=self._token_manager,
                        org_name=self._org_name,
                        repo_name=self._repo_name,
                        file_path=file_path
                    )
                )
        else:
            new = False
            updated = False
        result = RepoDetailResult(
            org_name=self._org_name,
            succeed=True,
            fail_reason=None,
            repo_name=self._repo_name,
            data=Repo(
                name=self._repo_name,
                org_name=self._org_name,
                created_at=datetime.fromisoformat(response_body["created_at"]).replace(tzinfo=None),
                updated_at=last_push_datetime,
                html_url=response_body["html_url"],
                api_url=response_body["url"],
                updated=updated,
                new=new,
            )
        )
        return result, requests_ or None


class RepoFileRequest(DataFetchRequest):
    def __init__(self, token_manager: GitHubTokenManager, org_name: str, repo_name: str, file_path: str):
        super().__init__(token_manager=token_manager)
        self._org_name = org_name
        self._repo_name = repo_name
        self._file_path = file_path

    @property
    def url(self):
        return self._get_url(f"repos/{self._org_name}/{self._repo_name}/contents/{self._file_path}")

    def _handle_no_token(self, response_body, **kwargs) -> tuple[RepoFileResult, None]:
        return RepoFileResult(
            org_name=self._org_name,
            repo_name=self._repo_name,
            succeed=False,
            fail_reason=DataFetchFailReason.NO_VALID_TOKEN,
            file_path=self._file_path,
        ), None

    def _handle_invalid_token(self, response_body, **kwargs) -> tuple[RepoFileResult, None]:
        return RepoFileResult(
            org_name=self._org_name,
            repo_name=self._repo_name,
            succeed=False,
            fail_reason=DataFetchFailReason.NO_VALID_TOKEN,
            file_path=self._file_path,
        ), None

    def _handle_token_retry(self, response_body, **kwargs) -> tuple[None, list['RepoFileRequest']]:
        return (
            None,
            [
                RepoFileRequest(
                    token_manager=self._token_manager,
                    org_name=self._org_name,
                    repo_name=self._repo_name,
                    file_path=self._file_path,
                )
            ]
        )

    def _handle_status(self, response_status: int, response_body, **kwargs) -> tuple[RepoFileResult, None]:
        reason = DataFetchFailReason.NOT_FOUND if response_status == 404 else DataFetchFailReason.UNEXPECTED_STATUS
        return RepoFileResult(
            org_name=self._org_name,
            repo_name=self._repo_name,
            succeed=False,
            fail_reason=reason,
            file_path=self._file_path,
        ), None

    def _handle_valid_response(
        self, response_body, **kwargs
    ) -> tuple[RepoFileResult, None]:
        return RepoFileResult(
            org_name=self._org_name,
            succeed=True,
            fail_reason=None,
            repo_name=self._repo_name,
            file_path=self._file_path,
            data=RepoFile(
                repo_name=self._repo_name,
                file_path=self._file_path,
                content=base64.b64decode(response_body["content"]).decode("utf-8"),
            ),
        ), None


class OrganizationRequest(DataFetchRequest):
    def __init__(self, token_manager: GitHubTokenManager, org_name: str):
        super().__init__(token_manager=token_manager)
        self._org_name = org_name

    @property
    def url(self):
        return self._get_url(f"orgs/{self._org_name}")

    def _handle_no_token(self, response_body, **kwargs) -> tuple[OrganizationResult, None]:
        return OrganizationResult(
            org_name=self._org_name,
            succeed=False,
            fail_reason=DataFetchFailReason.NO_VALID_TOKEN,
        ), None

    def _handle_invalid_token(self, response_body, **kwargs) -> tuple[OrganizationResult, None]:
        return OrganizationResult(
            org_name=self._org_name,
            succeed=False,
            fail_reason=DataFetchFailReason.NO_VALID_TOKEN,
        ), None

    def _handle_token_retry(self, response_body, **kwargs) -> tuple[None, list['OrganizationRequest']]:
        return (
            None,
            [
                OrganizationRequest(
                    token_manager=self._token_manager,
                    org_name=self._org_name,
                )
            ]
        )

    def _handle_status(self, response_status: int, response_body, **kwargs) -> tuple[OrganizationResult, None]:
        reason = DataFetchFailReason.NOT_FOUND if response_status == 404 else DataFetchFailReason.UNEXPECTED_STATUS
        return OrganizationResult(
            org_name=self._org_name,
            succeed=False,
            fail_reason=reason,
        ), None

    def _handle_valid_response(
        self, response_body, **kwargs
    ) -> tuple[OrganizationResult, None]:
        return OrganizationResult(
            org_name=self._org_name,
            succeed=True,
            fail_reason=None,
            data=Organization(
                name=response_body["login"],
                private_repos=response_body["total_private_repos"],
                public_repos=response_body["public_repos"],
            ),
        ), None


class GitHub(GitHost):
    NAME = "github"
    BASE_URL = "https://api.github.com"

    def __init__(self, token_manager: GitHubTokenManager):
        self._token_manager = token_manager
        self._requests_processing = set()

    def get_organization(self, org_name: str) -> OrganizationResult:
        request = OrganizationRequest(token_manager=self._token_manager, org_name=org_name)
        while True:
            result: Optional[OrganizationResult]
            result, requests_ = request.make_with_requests()
            if requests_ is not None:
                request = requests_[0]
            else:
                return result

    async def get_repos(
        self,
        org_name: str,
        repos_last_update: dict[str, datetime],
        file_paths: Collection[str],
        filter_: RepoFilter,
    ) -> Generator[DataFetchResult, None, None]:
        request_q = asyncio.Queue()
        result_q = asyncio.Queue()
        organization = self.get_organization(org_name)
        if not organization.succeed:
            yield organization
        else:
            tasks = []
            repos_count = organization.data.private_repos + organization.data.public_repos
            async with aiohttp.ClientSession() as session:
                if isinstance(name_filter := filter_.get_filter("name"), In):
                    for repo_name in name_filter.value:
                        request_q.put_nowait(
                            RepoRequest(
                                token_manager=self._token_manager,
                                org_name=org_name,
                                repo_name=repo_name,
                                repo_last_update=None
                            )
                        )
                else:
                    page_size = 100
                    last_page = int(repos_count / page_size)
                    last_page = last_page if repos_count % page_size == 0 else last_page + 1
                    for page in range(1, last_page + 1):
                        request_q.put_nowait(
                            ReposListingRequest(
                                token_manager=self._token_manager,
                                org_name=org_name,
                                page=page,
                                filter_=filter_,
                                page_size=page_size,
                            )
                        )
                for _ in range(self._get_repo_workers(n_files=len(file_paths), filter_=filter_)):
                    tasks.append(
                        asyncio.create_task(
                            self._handle_repo_requests(
                                session=session,
                                request_q=request_q,
                                result_q=result_q,
                                repos_last_update=repos_last_update,
                                file_paths=file_paths,
                            )
                        )
                    )
                async for result in self._gather_repo_results(request_q, result_q, file_paths):
                    yield result
            await request_q.join()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    def _get_repo_workers(n_files: int, filter_: RepoFilter) -> int:
        workers_per_repo = n_files
        if isinstance(name_filter := filter_.get_filter("name"), In):
            workers_per_repo *= len(name_filter.value)
        else:
            workers_per_repo *= 100
        return workers_per_repo

    async def _gather_repo_results(
        self,
        request_q: asyncio.Queue,
        result_q: asyncio.Queue,
        file_paths: Optional[Iterable[str]]
    ) -> Generator[Union[RepoFullResult, ReposListingResult], None, None]:
        result_mergers: dict[str, RepoResultMerger] = {}
        while True:
            if request_q.empty() and result_q.empty():
                if not self._requests_processing:
                    break
                else:
                    await asyncio.sleep(0.5)
            else:
                result = await result_q.get()
                if isinstance(result, RepoResult):
                    if result.repo_name not in result_mergers:
                        result_mergers[result.repo_name] = RepoResultMerger(file_paths)
                    result_mergers[result.repo_name].add_result(result)
                    merged_result = result_mergers[result.repo_name].get_merged_result()
                    if merged_result is not None:
                        result_mergers.pop(result.repo_name)
                        result_q.put_nowait(merged_result)
                        yield merged_result
                elif isinstance(result, ReposListingResult):
                    yield result
                result_q.task_done()
        for merger in result_mergers.values():
            merged_result = merger.get_merged_result()
            if merged_result is None:
                raise InconsistentResultsError("some results are missing")
            yield merged_result

    async def _handle_repo_requests(
        self,
        repos_last_update: dict[str, datetime],
        file_paths: Iterable[str],
        session: aiohttp.ClientSession,
        request_q: asyncio.Queue,
        result_q: asyncio.Queue,
    ) -> None:
        while True:
            request: DataFetchRequest = await request_q.get()
            self._requests_processing.add(id(request))
            result, requests_ = await request.make_with_aiohttp(
                session=session,
                file_paths=file_paths,
                repos_last_update=repos_last_update
            )
            if result is not None:
                result_q.put_nowait(result)
            if requests_:
                for next_request in requests_:
                    request_q.put_nowait(next_request)
            self._requests_processing.remove(id(request))
            request_q.task_done()
