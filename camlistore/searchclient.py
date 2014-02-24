

class SearchClient(object):
    """
    Low-level interface to Camlistore indexer search operations.

    The indexer component visits all blobs in the store and infers
    connections between them based on its knowledge of certain schema
    formats.

    In particular, the indexer is responsible for tracking all of the
    modification claims for a permanode and providing its flattened
    attribute map for any given point in time.

    However, the indexer also has special knowledge of the conventions
    around storage of filesystems and can thus be a more convenient interface
    for filesystem traversal than the raw blob interface.

    Callers should not instantiate this class directly. Instead, call
    :py:func:`camlistore.connect` to obtain a
    :py:class:`camlistore.Connection`
    object and access :py:attr:`camlistore.Connection.searcher`.
    """

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
        """
        Run a query against the index, returning an iterable of
        :py:class:`SearchResult`.

        The given expression is just passed on verbatim to the underlying
        query interface.

        Query constraints are not yet supported.
        """
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

    def describe_blob(self, blobref):
        """
        Request a description of a particular blob, returning a
        :py:class:`BlobDescription` object.

        The "description" of a blob is the indexer's record of the blob,
        so it contains only the subset of information retained by the
        indexer. The level of detail in the returned object will thus
        depend on what the indexer knows about the given object.
        """
        import json
        req_url = self._make_url("camli/search/describe")
        resp = self.http_session.get(
            req_url,
            params={
                "blobref": blobref,
            },
        )

        if resp.status_code != 200:
            from camlistore.exceptions import ServerError
            raise ServerError(
                "Failed to describe %s: server returned %i %s" % (
                    blobref,
                    resp.status_code,
                    resp.reason,
                )
            )

        raw = json.loads(resp.content)
        my_raw = raw["meta"][blobref]
        other_raw = raw["meta"]
        return BlobDescription(
            self,
            my_raw,
            other_raw_dicts=other_raw,
        )

    def get_claims_for_permanode(self, blobref):
        """
        Get the claims for a particular permanode, as an iterable of
        :py:class:`ClaimMeta`.

        The concept of "claims" is what allows a permanode to appear
        mutable even though the underlying storage is immutable. The
        indexer processes each of the valid claims on a given permanode
        to produce an aggregated set of its attributes for a given point
        in time.

        Most callers should prefer to use :py:meth:`describe_blob` instead,
        since that returns the flattened result of processing all
        attributes, rather than requiring the client to process the claims
        itself.
        """
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
    """
    Represents a search result from :py:meth:`SearchClient.query`.
    """

    #: The blobref of the blob represented by this search result.
    blobref = None

    def __init__(self, blobref):
        self.blobref = blobref

    def __repr__(self):
        return "<camlistore.searchclient.SearchResult %s>" % self.blobref


class BlobDescription(object):
    """
    Represents the indexer's description of a blob, from
    :py:meth:`SearchClient.describe_blob`.
    """

    def __init__(self, searcher, raw_dict, other_raw_dicts={}):
        self.searcher = searcher
        self.raw_dict = raw_dict
        self.other_raw_dicts = other_raw_dicts

    @property
    def blobref(self):
        """
        The blobref of the blob being described.
        """
        return self.raw_dict.get("blobRef")

    @property
    def type(self):
        """
        The indexer's idea of the type of the blob.
        """
        return self.raw_dict.get("camliType")

    @property
    def size(self):
        """
        The indexer's idea of the size of the blob.
        """
        return self.raw_dict.get("size")

    # plus some other stuff that varies depending on type
    # https://github.com/bradfitz/camlistore/blob/
    # ca58231336e5711abacb059763beb06e8b2b1788/pkg/search/handler.go#L722

    def describe_another(self, blobref):
        """
        Obtain a description of another related blob.

        When asked for a description, the indexer also returns descriptions
        of some of the objects related to the requested object, such as
        the files in a directory.

        This interface allows callers to retrieve related objects while
        possibly making use of that already-retrieved data, falling back
        on a new call to the indexer if the requested blob was not already
        described.

        Since this method sometimes uses data retrieved earlier, it may return
        stale data. If the latest data is absolutely required, prefer to
        call directly :py:meth:`SearchClient.describe_blob`.
        """
        if blobref in self.other_raw_dicts:
            return BlobDescription(
                self.searcher,
                self.other_raw_dicts[blobref],
                self.other_raw_dicts,
            )
        else:
            return self.searcher.describe_blob(blobref)

    def __repr__(self):
        return "<camlistore.searchclient.BlobDescription %s %s>" % (
            self.type if self.type is not None else "(unknown)",
            self.blobref if self.blobref is not None else "(unknown)",
        )


class ClaimMeta(object):
    """
    Description of a claim.

    A claim is a description of a mutation against a permanode. The
    indexer aggregates claims to decide the state of a permanode
    for a given point in time.

    The :py:attr:`type` attribute represents the kind of mutation, and
    a different subset of the other attributes will be populated depending
    on that type.
    """

    def __init__(self, raw_dict):
        self.raw_dict = raw_dict

    @property
    def type(self):
        """
        The type of mutation being performed by this claim.
        """
        return self.raw_dict.get("type")

    @property
    def signer_blobref(self):
        """
        The blobref of the public key of the party that made this claim,
        against which the claim's signature was verified.
        """
        return self.raw_dict.get("signer")

    @property
    def attr(self):
        """
        For claims that mutate attributes, the name of the attribute that
        this claim mutates, as a string.
        """
        return str(self.raw_dict.get("attr"))

    @property
    def value(self):
        """
        For claims that mutate attributes, the value applied to the mutation.
        """
        return self.raw_dict.get("value")

    @property
    def blobref(self):
        """
        The blobref of the underlying claim object.
        """
        return self.raw_dict.get("blobref")

    @property
    def target_blobref(self):
        """
        For claim types that have target blobs, the blobref of the claim's
        target.
        """
        return self.raw_dict.get("target")

    @property
    def time(self):
        """
        The time at which the claim was made, as a
        :py:class:datetime.datetime:. The timestamps of claims are used
        to order them and to allow the indexer to decide the state of
        a permanode on any given date, by filtering later permanodes.
        """
        from dateutil.parser import parse

        raw = self.raw_dict.get("date")
        if raw is not None:
            return parse(raw)
        else:
            return None

    @property
    def permanode_blobref(self):
        """
        The blobref of the permanode to which this claim applies.
        """
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
