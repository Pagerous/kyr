from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Iterable, TypedDict

from sqlalchemy import (
    Enum,
    ForeignKey,
    String,
    UniqueConstraint,
    and_,
    delete,
    insert,
    select,
    update,
)
from sqlalchemy.orm import (
    Mapped,
    attribute_mapped_collection,
    declarative_base,
    keyfunc_mapping,
    mapped_column,
    relationship,
)

Base = declarative_base()


class GitHost(StrEnum):
    GITHUB = "github"

    @classmethod
    def values(cls):
        return [cls.GITHUB.value]


class Organization(Base):
    
    class RepoInsertData(TypedDict):
        name: str
        created_at: datetime
        last_commit_at: datetime
        html_url: str
        api_url: str
        
    class RepoUpdateData(TypedDict):
        name: str
        last_commit_at: datetime
        html_url: str
        api_url: str
        
    
    
    __tablename__ = "organization"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(256))
    git_host: Mapped[GitHost] = mapped_column(Enum(GitHost))
    private_repos: Mapped[int] = mapped_column()
    public_repos: Mapped[int] = mapped_column()
    repos: Mapped[dict[str, "Repo"]] = relationship(
        "Repo",
        back_populates="org",
        collection_class=attribute_mapped_collection("name"),
        cascade="all, delete-orphan",
    )

    __table_args__ = (UniqueConstraint(name, git_host),)

    @property
    def repos_count(self):
        return self.private_repos + self.public_repos
    
    @classmethod
    def get(
        cls, session, org_name: str, git_host_name: str
    ) -> Organization | None:
        return session.scalar(
            select(cls)
            .where(cls.name == org_name)
            .where(cls.git_host == git_host_name)
        )
        
    def delete_repos(self, session, repo_names: Iterable[str]):
        session.execute(
            delete(Repo).where(
                and_(
                    Repo.name.in_(repo_names),
                    Repo.org_id == self.id,
                )
            )
        )
        
    def insert_repos(self, session, repos_data: RepoInsertData):
        session.execute(
            insert(Repo),
            [
                {
                    "name": item["name"],
                    "org_id": self.id,
                    "created_at": item["created_at"],
                    "last_commit_at": item["last_commit_at"],
                    "html_url": item["html_url"],
                    "api_url": item["api_url"],
                }
                for item in repos_data
            ],
        )
        
    def update_repos(self, session, repos_data: Iterable[RepoUpdateData]):
        session.execute(
            update(Repo),
            [
                {
                    "id": self.repos[data["name"]].id,
                    "last_commit_at": data["last_commit_at"],
                    "html_url": data["html_url"],
                    "api_url": data["api_url"]
                }
                for data in repos_data
            ]
        )
        


class Repo(Base):
    __tablename__ = "repo"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(256))
    org_id: Mapped[str] = mapped_column(ForeignKey("organization.id"))
    org = relationship(
        "Organization",
        back_populates="repos",
    )
    created_at: Mapped[datetime] = mapped_column()
    last_commit_at: Mapped[datetime] = mapped_column()
    html_url: Mapped[str] = mapped_column(String(512))
    api_url: Mapped[str] = mapped_column(String(512))

    dependencies: Mapped[dict[tuple[str, str], "Dependency"]] = relationship(
        secondary="repo_dependency",
        back_populates="repos",
        collection_class=keyfunc_mapping(lambda e: (e.language, e.name)),
    )

    __table_args__ = (UniqueConstraint(name, org_id),)

    @property
    def key(self) -> tuple[int, str]:
        return (self.org_id, self.name)


class Dependency(Base):
    __tablename__ = "dependency"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(256))
    version: Mapped[str] = mapped_column(String(32))
    language: Mapped[str] = mapped_column(String(128))

    repos: Mapped[dict[tuple[int, str], "Repo"]] = relationship(
        secondary="repo_dependency",
        back_populates="dependencies",
        collection_class=keyfunc_mapping(lambda e: (e.org_id, e.name)),
    )

    __table_args__ = (UniqueConstraint(name, version, language),)

    @property
    def key(self) -> tuple[str, str]:
        return (self.language, self.name)


class RepoDependency(Base):
    __tablename__ = "repo_dependency"

    repo_id: Mapped[int] = mapped_column(ForeignKey(Repo.id), primary_key=True)
    dependency_id: Mapped[int] = mapped_column(
        ForeignKey(Dependency.id), primary_key=True
    )
