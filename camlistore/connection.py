
import json
import pkg_resources


version = pkg_resources.get_distribution("camlistore").version
user_agent = "python-camlistore/%s" % version


class Connection(object):

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
