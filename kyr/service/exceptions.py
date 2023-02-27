class PullError(Exception):
    def __init__(self, msg, error_code):
        super().__init__(msg)
        self.error_code = error_code


class GitHostException(Exception):
    pass


class MissingDataError(Exception):
    pass
