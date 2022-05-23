.. _usage:

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


Prepare To Lunch
================

| As said before, the most common use case for Lifeblood needs little to none configuration,
| However, if you plan to use default standard environment resolver - it needs to detect the location of
  the software it can resolve.

* TODO: add steps how to generate initial resolver config

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
