from pathlib import Path


PROJECT_DIRECTORY = Path.home().joinpath(".kyr/")


def ensure_project_directory():
    if not PROJECT_DIRECTORY.exists():
        PROJECT_DIRECTORY.mkdir()
