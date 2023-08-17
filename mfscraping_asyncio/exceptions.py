class DataDoesNotExist(Exception):
    pass


class LoginFailed(Exception):
    pass


class MFConnectionError(Exception):
    pass


class MFScraptingError(Exception):
    pass


class MFInitializeError(Exception):
    pass


class FetchTimeout(Exception):
    pass
