Getting Started
===============

Install The Module
------------------

This is a standard Python module and can be installed using ``pip`` as usual:

* ``pip install camlistore``

Connect To Camlistore
---------------------

Before this library will be of any use you will need a Camlistore server
running. See `the Camlistore docs`_ for more details.

Once you have a server running you can connect to it using
:py:func:`camlistore.connect`. For example, if you have your Camlistore
server running on localhost:

.. code-block:: python

    import camlistore

    conn = camlistore.connect("http://localhost:3179/")

This function will contact the specified URL to make sure it looks like
a valid Camlistore server and discover some details about its configuration.
The ``conn`` return value is then a :py:class:`camlistore.Connection` object,
configured and ready to access the server.

.. _`the Camlistore docs`: http://camlistore.org/docs/

Try Writing A Blob!
-------------------

To test if we've connected successfully, we can try some simple calls to
write a blob and retrieve it again:

.. code-block:: python

    blobref = conn.blobs.put(camlistore.Blob("Hello, Camlistore!"))
    hello_blob = conn.blobs.get(blobref)
    print hello_blob.data

If things are working as expected, this should print out
``Hello, Camlistore!``, having successfully written that string into the store
and retrieved it again. You're now ready to proceed to the following sections
to learn more about the blob store and search interfaces.

This program will fail if the connection is not configured properly. For
example, it may fail if the Camlistore server requires authentication, since
our example does not account for that.

Connection Interface Reference
------------------------------

.. autofunction:: camlistore.connect

.. autoclass:: camlistore.Connection
    :members:
