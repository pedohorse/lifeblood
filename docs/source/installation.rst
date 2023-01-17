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
while extra machines will only run workers.

Check :ref:`overview <overview-scheduler>` to learn basic components of Lifeblood

Also check the :ref:`tutorial section <tutorials_installation>`


Simplest: Installation scripts
==============================

the `github repo <https://github.com/pedohorse/lifeblood/tree/dev/install>`_ has an ``install`` folder,
that folder contains

* ``install.sh`` - for bash and linux (distribution does not matter, as long as python >=3.8 and <=3.10 is available)
* ``install.ps1`` - a powershell script for windows.

Just pick one of those scripts and put it into an empty folder.

Run it! (from console or just double click)

A fresh version of lifeblood will be brought from github and saved to a subfolder with name taken from the commit hash
(like 420deadbeef69), and ``lifeblood`` and ``lifeblood_viewer`` shell script files will be creating pointing to that
new version installed.

Notes
-----

* on Windows ``install.ps1`` script will download latest embedded python3 from python.org, so you do not have to
have python installed beforehand
* on Linux ``install.sh`` script will try to use system python and create a venv for lifeblood.

Troubleshooting
---------------

In case something went wrong don't hesitate to delete everything installed and start from scratch.

Simple: Using pip
=================

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