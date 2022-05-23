.. _scheduler:

===================
Lifeblood Scheduler
===================

.. contents::
    :local:

Scheduler is the central part of Lifeblood: it manages tasks and workers.

Generally there should always be only one single scheduler running in a local network.

Database
^^^^^^^^

when launched with ``lifeblood scheduler`` - scheduler will start with default database located at
``<config_dir>>/scheduler/main.db``

It is recommended to keep database on a fast drive (SSD is better than HDD), so you can adjust database location
in the config file ``<config_dir>>/scheduler/config.toml``

Broadcasting
^^^^^^^^^^^^

| By default Scheduler will start broadcasting it's address over the local network (every 10s by default)
| This way other components (worker, viewer) will be able to automatically connect to it when started
  from anywhere within the LAN.
| You should never start 2 or more broadcasting schedulers in the same LAN, otherwise their broadcasts will collide
  and workers and viewers will get confused.

You can disable broadasting in scheduler's config file, but then you will have to properly configure workers and viewers manually
so they know where to connect.

Logging
^^^^^^^

| All Lifeblood components log to stdout, as well as to a log file.
| Log files location is:

* ``/home/<username>/.local/share/lifeblood/log`` (or ``$XDG_DATA_HOME`` if defined) (linux)
* ``C:\Users\<username>\AppData\Local\lifeblood\log`` (based on ``%LOCALAPPDATA%``) (windows)
* ``/Users/<username>/Library/Application Support/lifeblood/log`` (mac)

By default all components of Lifeblood are set to log only "important" messages (like warnings and errors),
but to figure out what's happening sometimes you would need a bit more information.

| All of Lifeblood's components can accept ``--loglevel`` parameter before command to specify minimum log level to, well, log.
| for example ``lifeblood scheduler --loglevel DEBUG`` will set default log level to DEBUG and you will see all the
  crap printed out into stdout
