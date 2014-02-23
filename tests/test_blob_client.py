
import unittest
from mock import MagicMock

from camlistore.blobclient import BlobClient, BlobMeta


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

    def test_enumerate(self):
        http_session = MagicMock()
        http_session.get = MagicMock()
        response = MagicMock()
        http_session.get.return_value = response

        response.status_code = 200
        response.content = """
        {
            "blobs": [
                {
                    "blobRef": "dummy1",
                    "size": 5
                },
                {
                    "blobRef": "dummy2",
                    "size": 9
                }
            ],
            "continueAfter": "dummy2"
        }
        """

        blobs = BlobClient(http_session, 'http://example.com/')
        iterable = blobs.enumerate()
        iterator = iterable.__iter__()

        blob_metas = []
        blob_metas.append(iterator.next())
        blob_metas.append(iterator.next())

        http_session.get.assert_called_with(
            'http://example.com/camli/enumerate-blobs'
        )

        # now set up for the second request
        http_session.get = MagicMock()
        response = MagicMock()
        http_session.get.return_value = response

        response.status_code = 200
        response.content = """
        {
            "blobs": [
                {
                    "blobRef": "dummy3",
                    "size": 17
                }
            ]
        }
        """

        blob_metas.append(iterator.next())

        self.assertRaises(
            StopIteration,
            lambda: iterator.next(),
        )

        self.assertEqual(
            [type(x) for x in blob_metas],
            [BlobMeta, BlobMeta, BlobMeta],
        )
        self.assertEqual(
            [x.blobref for x in blob_metas],
            ["dummy1", "dummy2", "dummy3"],
        )
        self.assertEqual(
            [x.size for x in blob_metas],
            [5, 9, 17],
        )

    def test_get_size_multi(self):
        http_session = MagicMock()
        http_session.post = MagicMock()
        response = MagicMock()
        http_session.post.return_value = response

        response.status_code = 200
        response.content = """
        {
            "stat": [
                {
                    "blobRef": "dummy1",
                    "size": 5
                },
                {
                    "blobRef": "dummy2",
                    "size": 9
                }
            ]
        }
        """

        blobs = BlobClient(http_session, 'http://example.com/')
        result = blobs.get_size_multi("dummy1", "dummy2")

        http_session.post.assert_called_with(
            "http://example.com/camli/stat",
            data={
                "camliversion": "1",
                "blob1": "dummy1",
                "blob2": "dummy2",
            },
        )

        self.assertEqual(
            result,
            {
                "dummy1": 5,
                "dummy2": 9,
            }
        )

    def test_put_multi(self):
        http_session = MagicMock()

        class MockBlobClient(BlobClient):
            get_size_multi = MagicMock()

        http_session.post = MagicMock()
        response = MagicMock()
        http_session.post.return_value = response

        response.status_code = 200

        MockBlobClient.get_size_multi.return_value = {
            "sha1-c9a291475b1bcaa4aa0c4cf459c29c2c52078949": 6,
            "sha1-403c716ea737afeb54f40549cdf5727f10ba6f18": 6,
            "sha1-1a434c0daa0b17e48abd4b59c632cf13501c7d24": None,
        }

        blobs = MockBlobClient(http_session, 'http://example.com/')
        result = blobs.put_multi("dummy1", "dummy2", "dummy3")

        MockBlobClient.get_size_multi.assert_called_with(
            'sha1-c9a291475b1bcaa4aa0c4cf459c29c2c52078949',
            'sha1-403c716ea737afeb54f40549cdf5727f10ba6f18',
            'sha1-1a434c0daa0b17e48abd4b59c632cf13501c7d24',
        )

        http_session.post.assert_called_with(
            "http://example.com/camli/upload",
            files={
                'sha1-1a434c0daa0b17e48abd4b59c632cf13501c7d24': (
                    'sha1-1a434c0daa0b17e48abd4b59c632cf13501c7d24',
                    'dummy3',
                    'application/octet-stream',
                )
            }
        )

        self.assertEqual(
            result,
            [
                'sha1-c9a291475b1bcaa4aa0c4cf459c29c2c52078949',
                'sha1-403c716ea737afeb54f40549cdf5727f10ba6f18',
                'sha1-1a434c0daa0b17e48abd4b59c632cf13501c7d24',
            ]
        )
