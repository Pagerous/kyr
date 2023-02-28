from dparse import filetypes, parse


class PoetryLockParser:
    file_type = filetypes.poetry_lock
    
    def __init__(self, content: str):
        self._parsed_content = parse(content, filetypes.poetry_lock)