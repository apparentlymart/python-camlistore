
import json
import pkg_resources


version = pkg_resources.get_distribution("camlistore").version
user_agent = "python-camlistore/%s" % version


class Connection(object):
    """
    Represents a logical connection to a camlistore server.

    Most callers should not instantiate this directly, but should instead
    use :py:func:`connect`, which implements the Camlistore server discovery
    protocol to auto-configure an instance of this class.

    Note that this does not imply a TCP or any other kind of socket connection,
    but merely some persistent state that will be used when making requests
    to the server. In particular, several consecutive requests via the
    same connection may be executed via a single keep-alive HTTP connection,
    reducing round-trip time.
    """

    #: Provides access to the server's blob store via an instance of
    #: :py:class:`camlistore.blobclient.BlobClient`.
    blobs = None

    #: Provides access to the server's search interface via an instance of
    #: :py:class:`camlistore.searchclient.SearchClient`.
    searcher = None

    def __init__(
        self,
        http_session=None,
        blob_root=None,
        search_root=None,
        sign_root=None,
    ):
        self.http_session = http_session
        self.blob_root = blob_root
        self.search_root = search_root
        self.sign_root = sign_root

        from camlistore.blobclient import BlobClient
        self.blobs = BlobClient(
            http_session=http_session,
            base_url=blob_root,
        )

        from camlistore.searchclient import SearchClient
        self.searcher = SearchClient(
            http_session=http_session,
            base_url=search_root,
        )


# Internals of the public "connect" function, split out so we can easily test
# it with a mock http_session while not making the public interface look weird.
def _connect(base_url, http_session):
    from urlparse import urljoin

    config_url = urljoin(base_url, '?camli.mode=config')
    config_resp = http_session.get(config_url)

    if config_resp.status_code != 200:
        from camlistore.exceptions import NotCamliServerError
        raise NotCamliServerError(
            "Configuration request returned %i %s" % (
                config_resp.status_code,
                config_resp.reason,
            )
        )

    # FIXME: Should verify that the response has the right Content-Type,
    # but right now the reference camlistore implementation returns
    # text/javascript rather than application/json, so want to confirm
    # that's expected before hard-coding it.

    try:
        raw_config = json.loads(config_resp.content)
    except ValueError:
        # Assume ValueError means JSON decoding failed, which means this
        # thing is not acting like a valid camli server.
        from camlistore.exceptions import NotCamliServerError
        raise NotCamliServerError(
            "Server did not return valid JSON at %s" % config_url
        )

    # If we were redirected anywhere during loading, use the final URL
    # as the basis for the rest of our work below.
    config_url = config_resp.url

    blob_root = None
    search_root = None
    sign_root = None

    if "blobRoot" in raw_config:
        blob_root = urljoin(config_url, raw_config["blobRoot"])

    if "searchRoot" in raw_config:
        search_root = urljoin(config_url, raw_config["searchRoot"])

    if "jsonSignRoot" in raw_config:
        sign_root = urljoin(config_url, raw_config["jsonSignRoot"])

    return Connection(
        http_session=http_session,
        blob_root=blob_root,
        search_root=search_root,
        sign_root=sign_root,
    )


def connect(base_url):
    """
    Create a connection to the Camlistore instance at the given base URL.

    This function implements the Camlistore discovery protocol to recognize
    a server and automatically determine which features are available,
    ultimately instantiating and returning a :py:class:`Connection` object.

    For now we assume an unauthenticated connection, which is generally
    only possible when connecting via ``localhost``. In future this function
    will be extended with some options for configuring authentication.
    """
    import requests

    http_session = requests.Session()
    http_session.trust_env = False
    http_session.headers["User-Agent"] = user_agent
    # TODO: let the caller pass in a trusted SSL cert and then turn
    # on SSL cert verification. Until we do that we're vulnerable to
    # certain types of MITM attack on our SSL connections.

    return _connect(
        base_url,
        http_session=http_session,
    )
