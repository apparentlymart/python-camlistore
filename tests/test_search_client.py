
import unittest
from mock import MagicMock

from camlistore.searchclient import SearchClient, ClaimMeta


class TestSearchClient(unittest.TestCase):

    def test_get_claims_for_permanode(self):
        http_session = MagicMock()
        http_session.get = MagicMock()

        response = MagicMock()
        http_session.get.return_value = response

        response.status_code = 200
        response.content = """
        {
            "claims": [
                {
                    "dummy": 1
                },
                {
                    "dummy": 2
                }
            ]
        }
        """

        searcher = SearchClient(
            http_session=http_session,
            base_url="http://example.com/s/",
        )

        claims = searcher.get_claims_for_permanode('dummy1')

        http_session.get.assert_called_with(
            'http://example.com/s/camli/search/claims',
            params={
                "permanode": "dummy1",
            },
        )

        self.assertEqual(
            [type(claim) for claim in claims],
            [ClaimMeta, ClaimMeta],
        )
        self.assertEqual(
            [claim.raw_dict["dummy"] for claim in claims],
            [1, 2],
        )


class TestClaimMeta(unittest.TestCase):

    def test_attrs(self):
        from dateutil.tz import tzutc
        from datetime import datetime

        raw_dict = {
            "blobref": "dummy-blobref",
            "signer": "dummy-signer",
            "permanode": "dummy-permanode",
            "date": "2013-02-13T12:32:34.123Z",
            "type": "dummy-type",
            "attr": "dummy-attr",
            "value": 12,  # to make sure this doesn't get coerced to string
            "target": "dummy-target",
        }
        claim_meta = ClaimMeta(raw_dict)

        self.assertEqual(
            claim_meta.blobref,
            "dummy-blobref",
        )
        self.assertEqual(
            claim_meta.signer_blobref,
            "dummy-signer",
        )
        self.assertEqual(
            claim_meta.permanode_blobref,
            "dummy-permanode",
        )
        self.assertEqual(
            claim_meta.date,
            datetime(
                2013, 2, 13, 12, 32, 34, 123000, tzinfo=tzutc(),
            ),
        )
        self.assertEqual(
            claim_meta.type,
            "dummy-type",
        )
        self.assertEqual(
            claim_meta.attr,
            "dummy-attr",
        )
        self.assertEqual(
            claim_meta.value,
            12,
        )
        self.assertEqual(
            type(claim_meta.value),
            int,
        )
        self.assertEqual(
            claim_meta.target_blobref,
            "dummy-target",
        )
