

class BlobClient(object):
    """
    Low-level interface to Camlistore's blob store interface.

    The blob store is the lowest-level Camlistore API and provides only
    for inserting and retrieving immutable, content-addressed blobs.

    All of the functionality of Camlistore builds on this abstraction, but
    most use-cases are better served by the *search* interface, which
    can be accessed via :py:attr:`camlistore.Connection.searcher`.

    Callers should not instantiate this class directly. Instead, call
    :py:func:`camlistore.connect` to obtain a
    :py:class:`camlistore.Connection`
    object and access :py:attr:`camlistore.Connection.blobs`.
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
                "Server does not support blob interface"
            )

    def _make_blob_url(self, blobref):
        # TODO: urlencode the blobref in case some future crazy hash
        # algorithm includes non-url-safe characters?
        return self._make_url('camli/' + blobref)

    def get(self, blobref):
        """
        Get the data for a blob, given its blobref.

        Returns a :py:class:`camlistore.Blob` instance describing the
        blob, or raises :py:class:`camlistore.exceptions.NotFoundError` if
        the given blobref is not known to the server.
        """
        blob_url = self._make_blob_url(blobref)
        resp = self.http_session.get(blob_url)
        if resp.status_code == 200:
            return Blob(resp.content, blobref=blobref)
        elif resp.status_code == 404:
            from camlistore.exceptions import NotFoundError
            raise NotFoundError(
                "Blob not found: %s" % blobref,
            )
        else:
            from camlistore.exceptions import ServerError
            raise ServerError(
                "Failed to get blob %s: server returned %i %s" % (
                    blobref,
                    resp.status_code,
                    resp.reason,
                )
            )

    def get_size(self, blobref):
        """
        Get the size of a blob, given its blobref.

        Returns the size of the blob as an :py:class:`int` in bytes,
        or raises :py:class:`camlistore.exceptions.NotFoundError` if
        the given blobref is not known to the server.
        """
        blob_url = self._make_blob_url(blobref)
        resp = self.http_session.request('HEAD', blob_url)
        if resp.status_code == 200:
            return int(resp.headers['content-length'])
        elif resp.status_code == 404:
            from camlistore.exceptions import NotFoundError
            raise NotFoundError(
                "Blob not found: %s" % blobref,
            )
        else:
            from camlistore.exceptions import ServerError
            raise ServerError(
                "Failed to get metadata for blob %s: server returned %i %s" % (
                    blobref,
                    resp.status_code,
                    resp.reason,
                )
            )

    def blob_exists(self, blobref):
        """
        Determine if a blob exists with the given blobref.

        Returns `True` if the blobref is known to the server, or `False`
        if it is not.

        To more efficiently test the presence of many blobs at once,
        it's better to use :py:meth:`get_size_multi`; known blobs will have
        a size, while unknown blobs will indicate ``None``.
        """
        from camlistore.exceptions import NotFoundError
        try:
            self.get_size(blobref)
        except NotFoundError:
            return False
        else:
            return True

    def enumerate(self):
        """
        Enumerate all of the blobs on the server, in blobref order.

        Returns an iterable over all of the blobs. The underlying server
        interface returns the resultset in chunks, so beginning iteration
        will cause one request but continued iteration may cause followup
        requests to retrieve additional chunks.

        Most applications do not need to enumerate all blobs and can instead
        use the facilities provided by the search interface. The enumeration
        interface exists primarily to enable the Camlistore indexer to build
        its search index, but may be useful for other alternative index
        implementations.
        """
        from urlparse import urljoin
        import json
        plain_enum_url = self._make_url("camli/enumerate-blobs")
        next_enum_url = plain_enum_url

        while next_enum_url is not None:

            resp = self.http_session.get(next_enum_url)
            if resp.status_code != 200:
                from camlistore.exceptions import ServerError
                raise ServerError(
                    "Failed to enumerate blobs from %s: got %i %s" % (
                        next_enum_url,
                        resp.status_code,
                        resp.reason,
                    )
                )

            data = json.loads(resp.content)

            if "continueAfter" in data:
                next_enum_url = urljoin(
                    plain_enum_url,
                    "?after=" + data["continueAfter"],
                )
            else:
                next_enum_url = None

            for raw_blob_reference in data["blobs"]:
                yield BlobMeta(
                    raw_blob_reference["blobRef"],
                    size=raw_blob_reference["size"],
                    blob_client=self,
                )

    def put(self, blob):
        """
        Write a single blob into the store.

        The blob must be given as a :py:class:`camlistore.Blob` instance.
        Returns the blobref of the created blob, which is guaranteed
        to match `blob.blobref` of the given blob.

        This function will first check with the server to see if it has the
        given blob, so it is not necessary for the caller to check for the
        existence of the blob before uploading.

        When writing many blobs at once -- a more common occurence than just
        one in most applications -- it is more efficient to use
        :py:meth:`put_multi`, since it is able to batch-upload blobs and
        reduce the number of round-trips required to complete the operation.
        """
        result = self.put_multi(blob)
        return result[0]

    def get_size_multi(self, *blobrefs):
        """
        Get the size of several blobs at once, given their blobrefs.

        This is a batch version of :py:meth:`get_size`, returning a
        mapping object whose keys are the request blobrefs and whose
        values are either the size of each corresponding blob or
        ``None`` if the blobref is not known to the server.
        """
        import json

        form_data = {}
        form_data["camliversion"] = "1"
        for i, blobref in enumerate(blobrefs):
            form_data["blob%i" % (i + 1)] = blobref

        stat_url = self._make_url('camli/stat')
        resp = self.http_session.post(stat_url, data=form_data)

        if resp.status_code != 200:
            from camlistore.exceptions import ServerError
            raise ServerError(
                "Failed to get sizes of blobs: got %i %s" % (
                    resp.status_code,
                    resp.reason,
                )
            )

        data = json.loads(resp.content)

        ret = {blobref: None for blobref in blobrefs}
        for raw_meta in data["stat"]:
            ret[raw_meta["blobRef"]] = int(raw_meta["size"])

        return ret

    def put_multi(self, *blobs):
        """
        Upload several blobs to the store.

        This is a batch version of :py:meth:`put`, uploading several
        blobs at once and returning a list of their blobrefs in the
        same order as they were provided in the arguments.

        At present this method does *not* correctly handle the protocol
        restriction that only 32MB of data can be uploaded at once, so
        this function will fail if that limit is exceeded. It is intended
        that this will be fixed in a future version.
        """
        import hashlib

        upload_url = self._make_url('camli/upload')

        blobrefs = [
            blob.blobref for blob in blobs
        ]

        sizes = self.get_size_multi(*blobrefs)

        files_to_post = {}

        for blob in blobs:
            blobref = blob.blobref

            if sizes[blobref] is not None:
                # Server already has this blob, so skip
                continue

            files_to_post[blobref] = (
                blobref,
                blob.data,
                'application/octet-stream',
            )

        if len(files_to_post) == 0:
            # Server already has everything, so nothing to do.
            return blobrefs

        # FIXME: We should detect if our total upload size is >32MB
        # and automatically split it into multiple requests, since the
        # protocol forbids upload payloads greater than 32MB.
        resp = self.http_session.post(upload_url, files=files_to_post)

        if resp.status_code != 200:
            from camlistore.exceptions import ServerError
            raise ServerError(
                "Failed to upload blobs: got %i %s" % (
                    resp.status_code,
                    resp.reason,
                )
            )

        return blobrefs


class Blob(object):
    """
    Represents a blob.

    A blob is really just a raw string of bytes, but this class exists
    to provide a convenient interface to make a blob and find its
    blobref and size.

    Although blobs are not mutable, instances of this class *are*. Mutating
    instances of this class (by assigning to :py:attr:`Blob.data` or
    :py:attr:`Blob.hash_func_name`) will change the blob's blobref, causing
    it to be a different blob as far as Camlistore is concerned, although
    it remains the same object as far as Python is concerned.

    Most callers should not pass a ``blobref`` argument to the initializer,
    since it can be computed automatically from the other arguments. If one
    *is* provided, it *must* match the provided data or else the
    :py:class:`camlistore.exceptions.HashMismatchError` exception will be
    raised, allowing callers to check for a hash mismatch as a side-effect.
    If a blobref is provided, its hash function overrides the value passed
    in as ``hash_func_name``.
    """

    def __init__(self, data, hash_func_name='sha1', blobref=None):
        self._blobref = blobref  # will be computed on first access
        self.data = data
        self.hash_func_name = hash_func_name
        if blobref is not None:
            (hash_func_name, hash) = blobref.split('-', 1)
            apparent_blobref = self.blobref
            if blobref != apparent_blobref:
                from camlistore.exceptions import HashMismatchError
                raise HashMismatchError(
                    "Expected blobref %s but provided data has blobref %s" % (
                        blobref,
                        apparent_blobref,
                    )
                )

    @property
    def blobref(self):
        """
        The blobref of this blob.

        This value will change each time either
        :py:attr:`data` or :py:attr:`hash_func_name` is modified,
        so callers should be careful about caching this value in a
        local variable if modifications are expected.
        """
        if self._blobref is None:
            import hashlib
            self._blobref = '-'.join([
                self._hash_func_name,
                hashlib.new(self._hash_func_name, self._data).hexdigest(),
            ])
            pass

        return self._blobref

    @property
    def size(self):
        """
        The size of the blob data, in bytes.
        """
        return len(self._data)

    @property
    def data(self):
        """
        The raw blob data, as a :py:class:`str`.

        Assigning to this property will change :py:attr:`blobref`, and
        effectively create a new blob as far as the server is concerned.
        """
        return self._data

    @data.setter
    def data(self, value):
        if type(value) is not str:
            raise TypeError('Blob data must be str, not %r' % type(value))
        self._data = value
        self._blobref = None  # force to be recomputed on next access

    @property
    def hash_func_name(self):
        """
        The name of the hash function to use for this blob's blobref.

        This must always be the name of a function that is supported by
        both the local :py:mod:`hashlib` *and* the Camlistore server.
        ``"sha1"`` is currently a safe choice for compatibility, and is thus
        the default. ``"sha256"`` will also work with the implementations
        available at the time of writing.

        Assigning to this property will change :py:attr:`blobref`, and
        effectively create a new blob as far as the server is concerned.
        """
        return self._hash_func_name

    @hash_func_name.setter
    def hash_func_name(self, value):
        if not isinstance(value, basestring):
            raise TypeError(
                'Hash function name must be string, not %r' % (
                    type(value)
                )
            )
        self._hash_func_name = value
        self._blobref = None  # force to be recomputed on next access


class BlobMeta(object):
    """
    Metadata about a blob.

    This is essentially a :py:class:`camlistore.Blob` object without the
    blob's data, for situations where we have the identity of a blob but
    have not yet retrieved it.

    Callers should not instantiate this class directly. It's intended only
    to be used as the return value of methods on :py:class:`BlobClient`.
    """

    #: The blobref of the blob being described.
    blobref = None

    #: The size of the blob being described, if known. ``None`` otherwise.
    size = None

    def __init__(self, blobref, size=None, blob_client=None):
        self.blobref = blobref
        self.size = size
        self.blob_client = blob_client

    def get_data(self):
        """
        Retrieve the blob described by this object.

        This will call to the server to obtain the given blob, with the
        same behavior as :py:meth:`BlobClient.get`.
        """
        return self.blob_client.get(self.blobref)

    def __repr__(self):
        return "<camlistore.blobclient.BlobMeta %s>" % self.blobref
