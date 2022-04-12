==========
How to Use
==========

.. contents::
    :local:

Usage
=====


All components of Lifeblood are configured by a number of configs files, some of those values can be overriden with command-line arguments

.. _config-dir:

Config location
---------------

Config files are generated and stored in location defined by ``LIFEBLOOD_CONFIG_LOCATION`` environment variable,
by default it's:

* ``/home/<username>/lifeblood`` (linux)
* ``C:\Users\<username>\Documents\lifeblood`` (windows)
* ``/Users/<username>/Library/Preferences/lifeblood`` (mac)

| All configurations use TOML config language.
| In future we will refer to config location as ``<config_dir>``

Detailed config file description see at:

* TODO: add config description for scheduler
* TODO: add config description for worker
* TODO: add config description for viewer

| Some parameters defined in config files can be overriden with command line arguments
| If scheduler is installed as pip package - a ``lifeblood`` command will be added to PATH.
| The component may be launched just by supplying it as argument to lifeblood, like
| ``lifeblood scheduler``
| use ``lifeblood <command> --help`` to get reference for each command

Start a Scheduler
-----------------

| Scheduler is the central part of Lifeblood: it manages tasks and workers.
| Generally there should always be only one single scheduler running in a local network.
| read more here :ref:`in overview <overview-scheduler>`

| sure, you could just start scheruler with simple command ``lifeblood scheduler``
| but let's first cover a couple of important moments:

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

Let's actually start it already
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As I've mentioned before, starting scheduler is simple:

* ``lifeblood scheduler`` - this will start scheduler with all settings taken from configs
* ``lifeblood --loglevel DEBUG scheduler`` - this would start scheduler with loglevel set to verbose DEBUG level.
  Note that ``--loglevel`` is passed before command, as it's a general lifeblood flag
* ``lifeblood --loglevel DEBUG scheduler --verbosity-pinger INFO`` - some subcomponents of scheduler can produce really
  too much noise in the log, so some of them can be explicitly set to use a different log level.
* ``lifeblood --loglevel DEBUG scheduler --verbosity-pinger INFO --db-path /path/to/database.db`` - in addition to all
  said above, here we override database location set in config to use sqlite db file at ``/path/to/database.db``

Start Workers
-------------

| Worker is the part of Lifeblood that is responsible for launching things as ordered by scheduler.
| You would have a single scheduler and multiple workers ran across local network.
| Worker knows about computational resources available on current computer, and reports that to scheduler.
| Multiple workers may be launched on the same machine, they should be able to agree with each other on how to share
  resources among each other
| Workers can be started on the same machine with scheduler, BUT you should be careful to leave enough resources for
  scheduler to work. If machine runs out of memory - scheduler will have problems working and may crash
  or be killed by system.

currently worker requires:
* existing temporary directory, shared between all worker instances
* all worker instances must share the same process namespace

* ``lifeblood worker``
* ``lifeblood viewer`` (if lifeblood_viewer package is also installed)

here you would also supply component-specific command line arguments, for example

* ``lifeblood --loglevel DEBUG worker``
* ``lifeblood --loglevel DEBUG scheduler --verbosity-pinger INFO --db-path path/to/database.db``


