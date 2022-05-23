.. _scheduler:

===================
Lifeblood Scheduler
===================

.. contents::
    :local:

Scheduler is the central part of Lifeblood: it manages tasks and workers.

Generally there should always be only one single scheduler running in a local network.

Database
========

when launched with ``lifeblood scheduler`` - scheduler will start with default database located at
:ref:`config-dir <config-dir>` ``/scheduler/main.db``

It is recommended to keep database on a fast drive (SSD is better than HDD), so you can adjust database location
in the config file ``<config_dir>>/scheduler/config.toml``

Broadcasting
============

| By default Scheduler will start broadcasting it's address over the local network (every 10s by default)
| This way other components (worker, viewer) will be able to automatically connect to it when started
  from anywhere within the LAN.
| You should never start 2 or more broadcasting schedulers in the same LAN, otherwise their broadcasts will collide
  and workers and viewers will get confused.

You can disable broadcasting in scheduler's config file, but then you will have to properly configure workers and viewers manually
so they know where to connect.

.. include:: logging.rst