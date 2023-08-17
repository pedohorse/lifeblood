.. _viewer:

================
Lifeblood Viewer
================
.. toctree::
    :caption: Viewer
    :maxdepth: 1

    viewer_api
    ../../expressions

--------
Overview
--------

When you start viewer, you will see something like this:

.. image:: /images/viewer_overview.png
    :width: 100%

There are 3 main areas of the window:

* Node view (the main central widget)
* Task Groups list (to the left)
* Worker list (on the bottom)

-------
Widgets
-------

**********
Group List
**********

Tasks are grouped in... groups. One task may belong to zero or more groups,
but usually it's just one.

There are several columns there:

* prog (short for progress) shows the progress summary of the tasks in this group.
  you will see 4 numbers there, similar to ``1:2:3/6``. Those numbers mean
  ``<in progress>:<errored>:<done>/<total>`` number of tasks.

  The color would mean:

  * white: all tasks are idle
  * yellow: has tasks in progress
  * red: has errored tasks
  * green: all tasks have finished

* name - just the group's name
* creation time - time when the group was create (not tasks of the group)
* priority - group priority sets the base priority for it's tasks.
  In case task belongs to multiple groups - maximum of group priorities is taken
