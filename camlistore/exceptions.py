
class ConnectionError(Exception):
    pass


class NotCamliServerError(ConnectionError):
    pass


class NotFoundError(Exception):
    pass


class ServerError(Exception):
    pass


class ServerFeatureUnavailableError(Exception):
    pass
