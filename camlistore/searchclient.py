

class SearchClient(object):

    def __init__(self, http_session, base_url):
        self.http_session = http_session
        self.base_url = base_url

    def _make_url(self, path):
        if self.base_url is not None:
            from urlparse import urljoin
            return urljoin(self.base_url, path)
        else:
            from camlistore.exceptions import ServerFeatureUnavailableError
            raise ServerFeatureUnavailableError(
                "Server does not support search interface"
            )

    def query(self, expression):
        import json
        req_url = self._make_url("camli/search/query")

        data = {
            # TODO: Understand how constraints work and implement them
            # https://github.com/bradfitz/camlistore/blob/
            # ca58231336e5711abacb059763beb06e8b2b1788/pkg/search/query.go#L255
            #"constraint": "",
            "expression": expression,
        }

        resp = self.http_session.post(
            req_url,
            data=json.dumps(data),
        )

        if resp.status_code != 200:
            from camlistore.exceptions import ServerError
            raise ServerError(
                "Failed to search for %r: server returned %i %s" % (
                    expression,
                    resp.status_code,
                    resp.reason,
                )
            )

        raw_data = json.loads(resp.content)

        return [
            SearchResult(x["blob"]) for x in raw_data["blobs"]
        ]

    def get_claims_for_permanode(self, blobref):
        import json
        req_url = self._make_url("camli/search/claims")
        resp = self.http_session.get(
            req_url,
            params={"permanode": blobref},
        )

        if resp.status_code != 200:
            from camlistore.exceptions import ServerError
            raise ServerError(
                "Failed to get claims for %s: server returned %i %s" % (
                    blobref,
                    resp.status_code,
                    resp.reason,
                )
            )

        raw = json.loads(resp.content)
        return [
            ClaimMeta(x) for x in raw["claims"]
        ]


class SearchResult(object):

    def __init__(self, blobref):
        self.blobref = blobref

    def __repr__(self):
        return "<camlistore.searchclient.SearchResult %s>" % self.blobref


class ClaimMeta(object):

    def __init__(self, raw_dict):
        self.raw_dict = raw_dict

    @property
    def type(self):
        return self.raw_dict.get("type")

    @property
    def signer_blobref(self):
        return self.raw_dict.get("signer")

    @property
    def attr(self):
        return str(self.raw_dict.get("attr"))

    @property
    def value(self):
        return self.raw_dict.get("value")

    @property
    def blobref(self):
        return self.raw_dict.get("blobref")

    @property
    def target_blobref(self):
        return self.raw_dict.get("target")

    @property
    def date(self):
        from dateutil.parser import parse

        raw = self.raw_dict.get("date")
        if raw is not None:
            return parse(raw)
        else:
            return None

    @property
    def permanode_blobref(self):
        return self.raw_dict.get("permanode")

    def __repr__(self):
        parts = ["camlistore.searchclient.ClaimMeta", self.type]
        attr = self.attr
        value = self.value
        target = self.target_blobref
        if attr is not None:
            parts.append(attr + ":")
        if value is not None:
            parts.append(repr(value))
        if target is not None:
            parts.append(target)
        return "<%s>" % " ".join(parts)
