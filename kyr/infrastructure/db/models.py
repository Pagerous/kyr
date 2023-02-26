from datetime import datetime
from enum import StrEnum
from typing import List

from sqlalchemy import Enum, ForeignKey, ForeignKeyConstraint, String
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import declarative_base, relationship, Mapped, mapped_column


Base = declarative_base()


class GitHost(StrEnum):
    GITHUB = "github"

    @classmethod
    def values(cls):
        return [cls.GITHUB.value]


class Organization(Base):
    __tablename__ = "organization"

    name: Mapped[str] = mapped_column(String(256), primary_key=True)
    git_host: Mapped[GitHost] = mapped_column(Enum(GitHost), primary_key=True)
    private_repos: Mapped[int] = mapped_column()
    public_repos: Mapped[int] = mapped_column()
    repos: Mapped[List["Repo"]] = relationship(
        "Repo",
        back_populates="org",
        cascade="all, delete-orphan",
        foreign_keys="[Repo.org_name, Repo.git_host]"
    )


class Repo(Base):
    __tablename__ = "repo"

    name: Mapped[str] = mapped_column(String(256), primary_key=True)
    org_name: Mapped[str] = mapped_column(ForeignKey("organization.name"))
    git_host: Mapped[GitHost] = mapped_column(ForeignKey("organization.git_host"))
    org = relationship(
        "Organization", back_populates="repos", foreign_keys=[org_name, git_host]
    )
    created_at: Mapped[datetime] = mapped_column()
    updated_at: Mapped[datetime] = mapped_column()
    html_url: Mapped[str] = mapped_column(String(512))
    api_url: Mapped[str] = mapped_column(String(512))

    __table_args__ = (
        ForeignKeyConstraint([org_name, git_host], [Organization.name, Organization.git_host]),
    )

    @classmethod
    def primary_keys(cls):
        return [key.name for key in inspect(cls).primary_key]

    @classmethod
    def update_columns(cls):
        mapper = inspect(cls)
        return {
            "updated_at": mapper.c.updated_at,
            "html_url": mapper.c.html_url,
            "api_url": mapper.c.api_url
        }
