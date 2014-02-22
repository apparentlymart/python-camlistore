
import unittest
from mock import MagicMock

from camlistore.blobclient import BlobClient


class TestBlobClient(unittest.TestCase):

    def test_url_building(self):
        http_session = MagicMock()
        blobs = BlobClient(
            http_session=http_session,
            base_url="http://example.com/dummy-blobs/",
        )
        self.assertEqual(
            blobs._make_blob_url('dummy-blobref'),
            'http://example.com/dummy-blobs/camli/dummy-blobref',
        )

    def test_unavailable(self):
        http_session = MagicMock()
        blobs = BlobClient(
            http_session=http_session,
            base_url=None,
        )
        from camlistore.exceptions import ServerFeatureUnavailableError
        self.assertRaises(
            ServerFeatureUnavailableError,
            lambda: blobs._make_blob_url('dummy-blobref'),
        )

    def test_get_success(self):
        http_session = MagicMock()
        http_session.get = MagicMock()
        response = MagicMock()
        http_session.get.return_value = response

        response.status_code = 200
        response.content = 'dummy blob'

        blobs = BlobClient(
            http_session,
            'http://example.com/blerbs/',
        )
        result = blobs.get('dummy-blobref')

        http_session.get.assert_called_with(
            "http://example.com/blerbs/camli/dummy-blobref"
        )
        self.assertEqual(
            result,
            'dummy blob',
        )

    def test_get_not_found(self):
        http_session = MagicMock()
        http_session.get = MagicMock()
        response = MagicMock()
        http_session.get.return_value = response

        response.status_code = 404
        response.content = 'not found'

        blobs = BlobClient(
            http_session,
            'http://example.com/blerbs/',
        )
        from camlistore.exceptions import NotFoundError
        self.assertRaises(
            NotFoundError,
            lambda: blobs.get('dummy-blobref'),
        )
        http_session.get.assert_called_with(
            "http://example.com/blerbs/camli/dummy-blobref"
        )

    def test_get_server_error(self):
        http_session = MagicMock()
        http_session.get = MagicMock()
        response = MagicMock()
        http_session.get.return_value = response

        response.status_code = 500
        response.content = 'error'

        blobs = BlobClient(
            http_session,
            'http://example.com/blerbs/',
        )
        from camlistore.exceptions import ServerError
        self.assertRaises(
            ServerError,
            lambda: blobs.get('dummy-blobref'),
        )
        http_session.get.assert_called_with(
            "http://example.com/blerbs/camli/dummy-blobref"
        )

    def test_get_size_success(self):
        http_session = MagicMock()
        http_session.request = MagicMock()
        response = MagicMock()
        http_session.request.return_value = response

        response.status_code = 200
        response.headers = {
            'content-length': 5
        }

        blobs = BlobClient(
            http_session,
            'http://example.com/blerbs/',
        )
        result = blobs.get_size('dummy-blobref')

        http_session.request.assert_called_with(
            "HEAD", "http://example.com/blerbs/camli/dummy-blobref"
        )
        self.assertEqual(
            result,
            5,
        )

    def test_blob_exists(self):
        http_session = MagicMock()

        class MockBlobClient(BlobClient):
            get_size = MagicMock()

        MockBlobClient.get_size.return_value = 12

        blobs = MockBlobClient(http_session, 'baz')
        result = blobs.blob_exists('foo')

        self.assertEqual(
            result,
            True,
        )
        MockBlobClient.get_size.assert_called_with(
            'foo',
        )

        from camlistore.exceptions import NotFoundError
        MockBlobClient.get_size.side_effect = NotFoundError('dummy')

        result = blobs.blob_exists('foo')
        self.assertEqual(
            result,
            False,
        )
