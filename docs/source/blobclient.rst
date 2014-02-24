Accessing the Blob Store
========================

The lowest-level interface in Camlistore is the raw blob store, which provides
a mechanism to store and retrieve immutable objects. All other Camlistore
functionality is built on this base layer.

Blob store functionality is accessed via
:py:attr:`camlistore.Connection.blobs`, which is a pre-configured instance
of :py:class:`camlistore.blobclient.BlobClient`.

.. autoclass:: camlistore.blobclient.BlobClient
   :members:

.. autoclass:: camlistore.Blob
   :members:

.. autoclass:: camlistore.blobclient.BlobMeta
   :members:
