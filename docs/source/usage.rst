==========
How to Use
==========

.. contents::
    :local:

Step by step guide
******************

Installation
============

Lifeblood consists of 2 parts:

* the core part that contains scheduler and worker, and has no GUI dependencies
* viewer part, you only need viewer on user machines that will need to see the progress of their tasks
  and modify task nodetree

The simplest way to install Lifeblood is to use pip:

* on machines that will be performing work - install just Lifeblood: ``pip install lifeblood``
* on machines that will need GUI to access scheduler - install viewer: ``pip install lifeblood_viewer``

A common simplest case for one artist with 1-2 extra computers on the network would be
to install Lifeblood_viewer (it will also install Lifeblood itself), and install just lifeblood
on those extra machines.

In this simplest case you will probably run scheduler, a worker pool and viewer on the main machine,
while extra machines will only run workers.

Check :ref:`overview <overview-scheduler>` to learn basic components of Lifeblood

Configuration
=============

Lifeblood is made in a way so you can start it with minimum to zero pre-configuration.
But even in simplest cases you might will need to adjust something.

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

Launch
======

You will need to launch Lifeblood's components.

* single scheduler
* any number of workers
* veiwers if needed and as needed

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

It's important to understand: one worker can launch only one payload process at one time,
there is no concept of slots. Instead, workers running locally share resources among each other.

So all locally running workers will inform each other about the amount of resources that the task they
execute requested, sharing all machine resources without overspending.

On a machine with a lot of CPU cores for example it makes sense to have more than one workers running
to use resources efficiently, and it can be tedious to manually launch all workers.

For that reason worker pools exist. Generally it's a bigger concept, in this case we need a simplest worker pool
implementation - simple local worker pool.

This pool operates according to the following logic:

* if there is no free workers and we are not at worker limit - launch a worker
* a worker that finishes a task terminates

well, that's it, not much complicated logic in there, but it's more powerful than a worker with constant number of slots:

* pool launches a worker (let's call it worker1), it advertises to scheduler all of it's (for example) 32 cores and 64 GBs of ram
* scheduler assigns worker1 a task that requires 8 cpu cores adn 16GB ram
* there's no free workers now, so pool launches another worker (worker2). worker2 will communicate with worker1
  and learn about the task it's executing, so worker2 will only advertise 24 cores and 48 GBs of ram to the scheduler
* scheduler assigns worker2 a task that requires 20 cores and 40GB of ram
* there's no free workers now, so pool launches another worker (worker3). worker3 will communicate with worker1
  and worker2 and set it's resources to 4 cores and 8 GBs of ram
* now worker1 finishes task and terminates, worker2 and worker3 are informed, and both of them adjust the resources
  advertised to scheduler to 12 cores and 24 GB of ram, though worker2 is still working on it's task
* worker2 finishes task and terminates, worker3 is informed, so it adjusts resources advertised to scheduler to full
  machine's resources: 32 cores and 64 GBs of ram.

and this can get as granular as resources allow. That's why always make sure to set up non zero resources usage for your tasks
and limit maximum workers that pool can spawn to some sane and safe amount.
