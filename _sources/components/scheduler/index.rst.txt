.. _scheduler:

=========
Scheduler
=========

.. toctree::
    :caption: Scheduler
    :maxdepth: 1
    :hidden:

    scheduler_configs

Scheduler is the central part of Lifeblood: it manages tasks and workers, viewers connect to it.

Refer to :ref:`overview` to understand the main concepts.

.. warning::
    Since Scheduler manages all the resources, there should only be **one single scheduler** running on your network.

First of all, you need to make sure that every component is configured to reach scheduler.
This may be done by manual configuration, or by relying on automatic **broadcasting**.

.. _scheduler broadcasting:

Broadcasting
============

| By default Scheduler will start broadcasting it's address over the local network (every 10s by default),
  and by default every component will start listening to scheduler's broadcasts.
| This way other components (worker, viewer) will be able to automatically connect to it when started
  from anywhere within the LAN.
| You should never start 2 or more broadcasting schedulers in the same LAN, otherwise their broadcasts will collide
  and workers and viewers will get confused.

You can disable broadcasting in scheduler's config file, but then you will have to properly configure workers and viewers manually
so they know where to connect.

.. note::
    usually networks are configured so that broadcast is not propagated through bridges and gateways,
    so if you have some complicated network topology - you need to configure components manually.
    Defaults work best for small home/company networks.

But if you do want to change the defaults and disable broadcasting, default scheduler config comes with
commented preset for this.

* To disable the broadcasting, just uncomment this line ``broadcast = false``
* To specify ip address to use ``server_ip = "192.168.0.234"`` (with your address obviously)
* You can specify a separate ip to be used for UI with ``ui_ip = "172.1.2.3"``, otherwise ``server_ip`` will be used

refer to :ref:`scheduler_configs` for details


.. _scheduler database:

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

.. _scheduler logging:

.. include:: ../logging.rst
