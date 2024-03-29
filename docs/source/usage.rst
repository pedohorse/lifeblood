.. _usage:

==========
How to Use
==========

.. contents::
    :local:

.. _configuration:

Configuration
=============

Lifeblood is made in a way so you can start it with minimum to zero pre-configuration.
But even in simplest cases you might will need to adjust something.

Lifeblood's components will create default configs for themselves on first launch.
You can start with default config and adjust it if such need arises.

All components of Lifeblood are configured by a number of configs files, some of those values can be overriden with command-line arguments

To configure your firewalls to allow lifeblood communications - see :ref:`network_config`

.. _config-dir:

Config location
---------------

Config files are generated and stored in location defined by ``LIFEBLOOD_CONFIG_LOCATION`` environment variable,
by default it's:

* on linux it's ``~``: ``~/lifeblood``, ``/home/<username>/lifeblood`` (linux)
* on windows it's your ``Documents`` folder: ``%HOME%/lifeblood``, ``C:\Users\<username>\Documents\lifeblood`` (windows)
* on mac it's in ``~/Library/Preferences/lifeblood``, ``/Users/<username>/Library/Preferences/lifeblood`` (mac)

In your config location you will find subdirs named after components, and certain component-specific configuration files within.
The main config file in most cases is called ``config.toml``. It uses `toml language <https://toml.io/>`_.

| All configurations use TOML config language.
| In future we will refer to config location as ``<config_dir>``

Detailed config file description see at:

* :ref:`scheduler configs`
* :ref:`worker configs`
* :ref:`viewer configs`

| Some parameters defined in config files can be overriden with command line arguments
| If scheduler is installed as pip package - a ``lifeblood`` command will be added to PATH.
| The component may be launched just by supplying it as argument to lifeblood, like
| ``lifeblood scheduler``
| use ``lifeblood <command> --help`` to get reference for each command

Hardware Setup
==============

In this "How To" it's assumed that you have the simplest and most common individual network setup:

* All your computers on the network can directly communicate to each other
* | All your computers in the same subnetwork
  | This is required for broadcasting. To ease the setup scheduler will broadcast it's address into current subnetwork.
    Subnetwork targeted broadcasts should be forwarded by routers if needed, but it's safer (and common) to have
    all devices connected to the same router.
* | All your computers have some shared location accessible from all computers with the same address
  | This is a reasonable assumption as most commonly you would want to have uniform access to scenes,
    textures, models etc from all work machines, and so that ``path/to/texture1`` leads to the same texture file
    on all machines
* In case you have firewall setup - see :ref:`network_config` for details on what ports need to be opened where.

.. _usage_prepare_to_launch:

Prepare To Launch
=================

| As said before, the most common use case for Lifeblood needs little to none configuration,
| However, if you plan to use default standard environment resolver - it needs to detect the location of
  the software it can resolve.

When a worker started and cannot find configuration for :ref:`standard_environment_resolver` - it will generate one.
It will try it's best to look in the most common locations for certain software, but any small non-standard obstacle
will prevent auto-detection, so it is safer and better to run auto-detection manually and modify generated config if needed.

To perform software auto-detection run ``lifeblood resolver scan`` - this command will just print out generated config,
no files will be written. To write auto-detected configuration to the standard lifeblood's :ref:`config-dir` - run
``lifeblood resolver generate``.

To edit configuration file, you might want to check examples in :ref:`standard_environment_resolver`.

Configuration file is located at :ref:`config-dir` ``/standard_environment_resolver/config.toml``

How you fill it depends fully on your workflow, but, for **example**, in case of houdini, the submitter expects houdini package to look
something like this (versions adjusted of course):

.. code-block:: toml

    [packages."houdini.py3_10"."20.0.506"]
    label = "SideFX Houdini 20.0.506"
    env.PATH.prepend = "/opt/hfs20.0.506/bin"


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
| read more :ref:`in scheduler documentation <scheduler>`

Starting scheduler is simple:

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
  or be killed by the system.

Read more :ref:`in worker documentation <worker>`

.. _usage pools:

Use Pools instead
^^^^^^^^^^^^^^^^^

Instead of starting individual workers you will probably want to start some kind of worker pool that will manage workers
for you

If you just want to get it started:

  ``lifeblood pool simple``

That's it. This way a "simple" worker pool will be started. it creates new workers as long as no idle workers exist and
system has resources left to do work.

You can start individual workers yourself manually with, but simple cases should be covered by the pool

  ``lifeblood worker``

Start Viewer
------------

Viewer is a component that is used to connect to the scheduler and:

* see the progress of your tasks
* create node graph
* manipulate tasks

  ``lifeblood viewer``

Viewer is just a user interface, it's not needed for proper scheduler or worker operation.

You will have to use viewer to set up your Lifeblood task processing node network.
By default scheduler with a new database has no nodes, so no tasks can be created.

see :ref:`tutorials<tutorials_viewer>` to understand how to work in the viewer
