
class ConnectionError(Exception):
    """
    There was some kind of error while establishing an initial connection
    to a Camlistore server.
    """
    pass


class NotCamliServerError(ConnectionError):
    """
    When attempting to connect to a Camlistore server it was determined that
    the given resource does not implement the Camlistore protocol, and is
    thus assumed not to be a Camlistore server.
    """
    pass


class NotFoundError(Exception):
    """
    The requested object was not found on the server.
    """
    pass


class ServerError(Exception):
    """
    The server returned an unexpected error in response to some operation.
    """
    pass


class ServerFeatureUnavailableError(Exception):
    """
    The server does not implement the requested feature.

    This can occur if
    e.g. a particular server is running a blob store but is not running
    an indexer, and a caller tries to use search features.
    """
    pass


class HashMismatchError(Exception):
    """
    There was a mismatch between an expected hash value an an actual hash
    value.
    """
    pass
