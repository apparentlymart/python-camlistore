
import unittest
from mock import MagicMock

from camlistore.connection import _connect, Connection
from camlistore.exceptions import NotCamliServerError


class TestConnect(unittest.TestCase):

    def test_success(self):
        http_session = MagicMock()

        response = MagicMock()

        http_session.get = MagicMock()
        http_session.get.return_value = response

        response.status_code = 200
        response.content = """
            {
                "blobRoot": "/mock-blobs/",
                "searchRoot": "/mock-search/",
                "jsonSignRoot": "/mock-sign/"
            }
        """
        # Act as if we were redirected to example.net
        response.url = "http://example.net/?camli.mode=config"

        conn = _connect(
            'http://example.com/',
            http_session=http_session,
        )

        http_session.get.assert_called_with(
            'http://example.com/?camli.mode=config'
        )

        self.assertEqual(
            conn.blob_root,
            "http://example.net/mock-blobs/",
        )
        self.assertEqual(
            conn.search_root,
            "http://example.net/mock-search/",
        )
        self.assertEqual(
            conn.sign_root,
            "http://example.net/mock-sign/",
        )

    def test_not_found(self):

        http_session = MagicMock()

        response = MagicMock()

        http_session.get = MagicMock()
        http_session.get.return_value = response

        response.status_code = 404
        response.content = "Not Found"
        response.url = "http://example.com/?camli.mode=config"

        self.assertRaises(
            NotCamliServerError,
            lambda: _connect(
                'http://example.com/',
                http_session=http_session,
            ),
        )

    def test_not_json(self):

        http_session = MagicMock()

        response = MagicMock()

        http_session.get = MagicMock()
        http_session.get.return_value = response

        response.status_code = 200
        response.content = "la la i am not json"
        response.url = "http://example.com/?camli.mode=config"

        self.assertRaises(
            NotCamliServerError,
            lambda: _connect(
                'http://example.com/',
                http_session=http_session,
            ),
        )

    def test_missing_keys(self):

        http_session = MagicMock()

        response = MagicMock()

        http_session.get = MagicMock()
        http_session.get.return_value = response

        response.status_code = 200
        response.content = "{}"
        response.url = "http://example.com/?camli.mode=config"

        conn = _connect(
            'http://example.com/',
            http_session=http_session,
        )

        self.assertEqual(
            conn.blob_root,
            None,
        )
        self.assertEqual(
            conn.search_root,
            None,
        )
        self.assertEqual(
            conn.sign_root,
            None,
        )
