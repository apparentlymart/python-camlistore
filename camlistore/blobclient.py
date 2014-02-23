

class BlobClient(object):

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
        blob_url = self._make_blob_url(blobref)
        resp = self.http_session.get(blob_url)
        if resp.status_code == 200:
            return resp.content
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
        blob_url = self._make_blob_url(blobref)
        resp = self.http_session.request('HEAD', blob_url)
        if resp.status_code == 200:
            return resp.headers['content-length']
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
        from camlistore.exceptions import NotFoundError
        try:
            self.get_size(blobref)
        except NotFoundError:
            return False
        else:
            return True

    def enumerate(self):
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

    def put(self, payload, hash_algo_name='sha1'):
        self.put_multi(payload, hash_algo_name=hash_algo_name)

    def get_size_multi(self, *blobrefs):
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

    def put_multi(self, *payloads, **kwargs):
        import hashlib

        upload_url = self._make_url('camli/upload')

        hash_algo_name = kwargs.get('hash_algo_name', 'sha1')

        blobrefs = [
            '-'.join([
                hash_algo_name,
                hashlib.new(hash_algo_name, payload).hexdigest(),
            ])
            for payload in payloads
        ]

        sizes = self.get_size_multi(*blobrefs)

        files_to_post = {}

        for i, payload in enumerate(payloads):
            blobref = blobrefs[i]

            if sizes[blobref] is not None:
                # Server already has this blob, so skip
                continue

            files_to_post[blobref] = (
                blobref,
                payload,
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


class BlobMeta(object):

    def __init__(self, blobref, size=None, blob_client=None):
        self.blobref = blobref
        self.size = size
        self.blob_client = blob_client

    def get_data(self):
        return self.blob_client.get(self.blobref)

    def __repr__(self):
        return "<camlistore.blobclient.BlobMeta %s>" % self.blobref
