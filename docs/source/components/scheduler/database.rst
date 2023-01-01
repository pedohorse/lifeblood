

Database
========

Probably the most important part of the configuration is where your scheduler's database is stored.

A scheduler's database is something like a farm manager repository - it's where all nodes and tasks are stored.

Lifeblood uses `sqlite <https://www.sqlite.org/>`_ as it's database engine.

By default, the database file location is set to be next to scheduler's config file in :ref:`config-dir`

| However it may be overriden by config entry ``scheduler.database.path``, or command line argument ``--db-path``.
| It is quite reasonable to have multiple databases to test different things around.
| However the main idea of Lifeblood is to set up your pipeline logic in one single database.

.. note::
    it is better to create the database on an **SSD** drive if possible,
    as it will be much faster, especially on huge databases.
    Though database on HDD is quite fine for low task load, but the lag may be noticeable.

.. warning::
    do NOT run multiple schedulers with different databases at the same time. Though it is supported
    in general - it requires some configuration, like disabling broadcasting. So you need to know what you
    are doing
