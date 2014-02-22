
import unittest
from mock import MagicMock

from camlistore.connection import Connection


class TestConnection(unittest.TestCase):

    def test_blob_client(self):
        http_session = MagicMock()
        conn = Connection(
            http_session=http_session,
            blob_root='dummy',
        )
        blob_client = conn.blobs
        self.assertEqual(
            blob_client.base_url,
            'dummy',
        )
        self.assertEqual(
            blob_client.http_session,
            http_session,
        )
