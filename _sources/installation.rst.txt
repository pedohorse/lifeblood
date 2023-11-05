.. _installation:

============
Installation
============

Lifeblood consists of 2 parts:

* the core part that contains scheduler and worker, and has no GUI dependencies
* viewer part, you only need viewer on user machines that will need to see the progress of their tasks
  and modify task nodetree

Core part is light and has few dependencies. Core part has to be installed on (or otherwise accessible by)
every machine that is going to participate in Lifeblood.

Viewer is only needed on the machines of people that need to be able to view the task progress
and edit the node tree

A common simplest case for one artist with 1-2 extra computers on the network would be
to install Lifeblood Viewer (it will also install Lifeblood Core itself), and install just Lifeblood Core
on those extra machines.

In this simplest case you will probably run scheduler, a worker pool and viewer on the main machine,
while extra machines will only run worker pools.

Check :ref:`overview <overview-scheduler>` to learn basic components of Lifeblood

Also check the :ref:`tutorial section <tutorials_installation>`

Matrix Notifier installation is described in :ref:`matrix notifier help page <nodes/stock/matrixnotifier>`

Simplest: `Lifeblood Manager <https://github.com/pedohorse/lifeblood-manager/releases>`_
========================================================================================

There is a separate program made specifically to simplify Lifeblood installation and management: `Lifeblood-Manager <https://github.com/pedohorse/lifeblood-manager/releases>`_.
It is not written in python and statically linked specifically to minimize the system software requirements.
(so you can for example use it on a windows system without python installed, or on linux without any extra packages)

Put this program into an empty folder where you want to install lifeblood. You do **NOT** need superuser privileges to run this, Lifeblood-Manager only ever makes changes to
the directory you specify in it (by default it's the directory where the executable lives). So unless you want to install lifeblood into some "protected" locations, such as
``C:\Program Files\...``, or ``/opt``, you **do not need** to run it as administrator or root.

When you run it, it will (or at least should) by default set the ``base directory`` to the directory where you put the manager in

It downloads latest commit from Lifeblood repo and sets up a minimal venv for it to work. It also creates and maintains launch shortcuts ``lifeblood`` and ``lifeblood_viewer``.
After installation you can easily launch lifeblood components with them. Manager can easily switch which version those shortcuts point to.

So again:

* Put lifeblood-manager into an empty folder you want lifeblood to be installed
   * it's recommended **NOT** to use ``<home>/lifeblood``, as this is the place where lifeblood configs will be stored by default.
* run lifeblood-manager
* press ``download freshest`` button
* now you can use ``lifeblood`` and ``lifeblood_viewer`` scripts to run lifeblood components. See :ref:`usage section <usage_prepare_to_launch>` on how to use them.

.. image:: /images/manager_overview.png

* ``download freshest`` button will download latest commit from given branch (dev by default for now), and make it **current** (so ``lifeblood`` and ``lifeblood_viewer``
  links will point to it)
* ``rename selected`` allows you to rename downloaded dir into something human-readable, while preserving links and other things if needed
* ``make selected version current`` button switches selected version to be **current**, meaning ``lifeblood`` and ``lifeblood_viewer`` links will be pointing to that version.
  This way you can easily switch between experimental and stable versions, if needed.

Troubleshooting
---------------

In case something went wrong don't hesitate to delete everything installed and start from scratch. All configuration files live in different location, so there is nothing
"state" saved in this location, you can remove and reinstall any number of times, that should not make any difference.

Simple: Using pip
=================

.. note::
  pip package versions correspond to tags in the repository, so they are always behind the development edge.

The simplest way to install Lifeblood is to use pip:

* on machines that will be performing work - install just Lifeblood: ``pip install lifeblood``
* on machines that will need GUI to access scheduler - install viewer: ``pip install lifeblood_viewer``

Complicated: Custom way
=======================

If you know what you are doing and you just want the latest hottest Lifeblood python modules -
all you need is `in src folder of the repo <https://github.com/pedohorse/lifeblood/tree/dev/src>`_:

* ``lifeblood`` is the Lifeblood core module
* ``lifeblood_viewer`` is the Lifeblood Viewer module
* ``lifeblood_client`` is something you won't probably need, only if you are embedding lifeblood somewhere.
  lifeblood_client is a python module that contains client that can be used to create new tasks and query existing
  tasks in a Lifeblood Scheduler