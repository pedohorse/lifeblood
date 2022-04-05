=====================
Overview of Lifeblood
=====================

.. contents::
    :depth: 2
    :local:

Main components
===============

Lifeblood is a task processing system. It consists of 3 main component types:
* scheduler
* worker
* viewer

.. _overview-scheduler:

scheduler
---------
Scheduler is the main brain of Lifeblood.
It processes tasks, moves them around and schedules them for execution on workers.

worker
------
Worker is the one component that does the (usually) heavy work specified by tasks' invocations.

viewer
------
Viewer is the tool that allows a user to view scheduler's state.
With a viewer you can see the state of the processing of your task group, or modify or create new node setups,
move tasks around, or otherwise change things in scheduler.

-----

The components arranged into 2 packages:

* lifeblood: scheduler and worker
* lifeblood_viewer: viewer (requires lifeblood)

As you see, viewer is separated from main package to avoid GUI dependencies for scheduler and worker,
and to make lifeblood package lighter.

Configuration
^^^^^^^^^^^^^
Configuration for lifeblood components is done with config files. Config files are located by default in your user folder:

* on linux it's ``~`` (``~/lifeblood``) (i.e. ``/home/username/lifeblood``)
* on windows it's your ``Documents`` folder (``%HOME%/lifeblood``) (i.e. ``C:\Users\username\Documents\lifeblood``)
* on mac it's in ``~/Library/Preferences/lifeblood``

In your config location you will find subdirs named after components, and certain component-specific configuration files within.
The main config file in most cases is called ``config.toml``. It uses `toml language <https://toml.io/>`_.

Lifeblood's components will create default configs for themselves on first launch.
You can start with default config and adjust it for your needs.

Some configuration values may be overriden as command line arguments. See command specifications for details.

Database location
"""""""""""""""""
Probably the most important part of the configuration is where your scheduler's database is stored.
Lifeblood uses `sqlite <https://www.sqlite.org/>`_ as it's database engine.

By default, the database file location is set to be next to scheduler's config file.

However it may be overriden by config entry ``core.database.path``, or command line argument ``--db-path``.
It is quite reasonable to have multiple databases to test different things around.
However the main idea of Lifeblood, scheduler is to set up your pipeline logic in one single database.

    NOTE: it is better to create the database on an **SSD** drive if possible,
    as it will be much faster, especially on huge databases.
    Though database on HDD is quite fine for low task load.

-----

All of the components above may run on the same machine, or on different machines within the same local network (for now)

Generally, workers and viewers only need to be able to connect to scheduler, but certain types of tasks, like, for example,
distributed simulation in houdini, require workers to be able to communicate to each other as well. (this requirement may be lifted in the future, if needed)

By default scheduler will start broadcasting to your default interface it's ip and ports for any possible workers and viewers
running on the same local network - this eases the small setups as all you need to do is just to launch components and they
will find each other automatically.
You can change that behaviour in scheduler's config. in ``core`` section ``broadcast = false`` would disable the broadcast,
with ``server_ip``, ``server_port`` you can override ip and port to use for worker connections, and with ``ui_ip``, ``ui_port``
you can override ip and port to use for viewer connections.

    Note: **IF BROADCASTING IS OFF** it is quite possible to run multiple schedulers on the same network (though you should not have a need to do it),
    or even on the same machine (but they MUST use different databases).
    But if broadcasting if enabled - multiple schedulers on the same local network will start to conflict with each other.
    Again: you should NOT have a need to run multiple schedulers.

so example config might look like this:

::

  [core]
    broadcast = false
    server_ip = "192.168.0.2"
    server_port = 7979
    ui_ip = "192.168.0.2"
    ui_port = 7989

Requirements
============
python3.6 or higher is required

OS support
----------
There is nothing strictly os-specific, except signal handling and process managing by the worker.

Currently it was **only tested in linux**.

MacOS, being posix, should theoretically work without problems too.
Windows requires some os-specific modifications and testing.

Installation
============

* lifeblood package (scheduler and worker) can be installed from pip: ``pip install lifeblood``, or ``python3 -m pip install lifeblood``
* lifeblood viewer (viewer, will also install lifeblood package) can be installed from pip: ``pip install lifeblood_viewer`` or ``python3 -m pip install lifeblood``
