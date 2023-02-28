from datetime import datetime
from enum import StrEnum

from sqlalchemy import Enum, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import (
    Mapped,
    attribute_mapped_collection,
    declarative_base,
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
    
    __table_args__ = (
        UniqueConstraint(name, git_host),
    )

    @property
    def repos_count(self):
        return self.private_repos + self.public_repos


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
    updated_at: Mapped[datetime] = mapped_column()
    html_url: Mapped[str] = mapped_column(String(512))
    api_url: Mapped[str] = mapped_column(String(512))
    
    dependencies: Mapped[
        dict[tuple[str, str, str], "Dependency"]
    ] = relationship(
        secondary="repo_dependency",
        back_populates="repos",
        collection_class=attribute_mapped_collection("key")
    )

    __table_args__ = (
        UniqueConstraint(name, org_id),
    )
    
    @property
    def key(self):
        return self.org_id, self.name


class Dependency(Base):
    __tablename__ = "dependency"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(256))
    version: Mapped[str] = mapped_column(String(32))
    language: Mapped[str] = mapped_column(String(128))
    
    repos: Mapped[
        dict[tuple[int, str], "Repo"]
    ] = relationship(
        secondary="repo_dependency",
        back_populates="dependencies",
        collection_class=attribute_mapped_collection("key")
    )
    
    __table_args__ = (
        UniqueConstraint(name, version, language),
    )
    
    @property
    def key(self) -> tuple[str, str, str]:
        return self.language, self.name, self.version
    
    
class RepoDependency(Base):
    __tablename__ = "repo_dependency"
    
    repo_id: Mapped[int] = mapped_column(ForeignKey(Repo.id), primary_key=True)
    dependency_id: Mapped[int] = mapped_column(
        ForeignKey(Dependency.id), primary_key=True
    )
    
    