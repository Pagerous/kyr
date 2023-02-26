from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from kyr.config import Config
from kyr.infrastructure.db.models import Base


_ENGINE = create_engine(f"sqlite:///{Config.get('db.location')}")
Base.metadata.create_all(_ENGINE)


def get_session():
    return Session(_ENGINE)
