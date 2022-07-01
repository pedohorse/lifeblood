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

:ref:`scheduler <scheduler>`
----------------------------
Scheduler is the main brain of Lifeblood.
It processes tasks, moves them around and schedules them for execution on workers.

:ref:`worker <worker>`
----------------------
Worker is the one component that does the (usually) heavy work specified by tasks' invocations.

:ref:`viewer <viewer>`
----------------------
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
Configuration for lifeblood components is done with config files. Config files are located by default in your user folder,
see :ref:`config-dir`


Database location
"""""""""""""""""

A scheduler's database is something like a farm manager repository - it's where all nodes and tasks are stored.

You can have multiple databases and switch between them, but at one time only one may be used.

Read more at :ref:`scheduler database`

.. warning::
    do NOT run multiple schedulers with different databases at the same time. Though it is supported
    in general - it requires some configuration, like disabling broadcasting. So you need to know what you
    are doing


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

.. note::
    **IF BROADCASTING IS OFF** it is quite possible to run multiple schedulers on the same network (though you should not have a need to do it),
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
python3.8 or higher is required

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
